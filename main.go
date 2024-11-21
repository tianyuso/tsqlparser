package main

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/tianyuso/tsqlparser/sqlparser"
)

type CTEInfo struct {
	Name  string
	Query string
}

func ParseCTEQuery(sql string) (ctes []CTEInfo, mainQuery string, err error) {
	var currentPos, startPos int
	var currentCTE string

	fmt.Printf("startPos: %d\n", startPos)
	// First, normalize the SQL string
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(strings.TrimSpace(sql), " ")

	// Simplified regex pattern for CTEs
	ctePattern := regexp.MustCompile(`(?i)([a-zA-Z0-9_]+)\s+AS\s*\(\s*(.*)\s*\)`)

	// Regular expression to split the WITH clause from the main query
	withPattern := regexp.MustCompile(`(?i)\s*WITH\s+(.*)\s*\)\s*(SELECT\s+.*$)`)

	// Match the overall WITH structure
	matches := withPattern.FindStringSubmatch(sql)
	if len(matches) != 3 {
		return nil, "", fmt.Errorf("invalid WITH clause structure")
	}
	for i, match := range matches {
		fmt.Printf("match %d: %s \n", i, match)

	}

	// Extract CTEs
	cteSection := matches[1]
	depth := 0

	// Manual parsing to handle nested parentheses
	for i, char := range cteSection {
		// fmt.Printf("char %d: %s\n", i, string(char))
		switch char {
		case '(':
			depth++
			if depth == 1 {
				startPos = i + 1
			}
		case ')':
			depth--
			if depth == 0 {
				currentCTE = cteSection[currentPos : i+1]
				fmt.Printf("currentCTE: %s\n", currentCTE)
				cteMatch := ctePattern.FindStringSubmatch(currentCTE)
				if len(cteMatch) == 3 {
					ctes = append(ctes, CTEInfo{
						Name:  strings.TrimSpace(cteMatch[1]),
						Query: strings.TrimSpace(cteMatch[2]),
					})
				}
				currentPos = i + 1
			}
		}
	}

	// Extract main query
	mainQuery = strings.TrimSpace(matches[2])

	return ctes, mainQuery, nil
}

func main() {
	// stmt, err := sqlparser.Parse("select * from ioa.dbo.user_items where user_id=1 order by created_at limit 3 offset 10")
	stmt, err := sqlparser.Parse("select * from dbo.users_u1 a")
	if err != nil {
		panic(err)
	}
	fmt.Printf("stmt = %+v\n", stmt)

	sql := `
with 
c1 as 
( 
    select * from db1.dbo.table1  t1
    inner join dbo.xx2 as x2 on x2.id= t1.xx2_id
    where x2.name like 'abc%' 
), 
ct2 as 
( 
    select * from dbo.table2 where id > 20 
), 
cte3 as 
( 
    select * from db44..table3
    left join table33  on table3.id = table33.t3_id 
    where price < 100 
) 
select a.id,b.name,c.order_id from c1 a, ct2 b, cte3 c where a.id = b.id and a.id = c.id 
union
SELECT u.id, u.name, o.order_id
FROM database1.schema1.users u
	LEFT JOIN schema2.orders o ON u.id = o.user_id
WHERE u.age > 18
	AND u.id IN (
		SELECT user_id
		FROM database2.schema2.transactions
		WHERE amount > 100
	)
UNION
SELECT a.id, a.name, b.order_id
FROM db2.schema3.account a
	INNER JOIN schema4.billing b ON a.id = b.account_id
`
	ctes, mainQuery, err := ParseCTEQuery(sql)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("CTEs:")
	for _, cte := range ctes {
		fmt.Printf("\nCTE Name: %s\nCTE Query:\n%s\n", cte.Name, cte.Query)
		stmt, err = sqlparser.Parse(cte.Query)
		if err != nil {
			fmt.Println("Failed to parse SQL:", err)
			return
		}

		// 输出解析结果
		fmt.Println("Databases, Schemas, Tables and Aliases:")
		switch stmtType := stmt.(type) {
		case *sqlparser.Select:
			ProcessSelectStatement(stmtType)
		case *sqlparser.Union:
			ProcessSelectStatement(stmtType)
		default:
			fmt.Println("Unsupported statement type")
		}
	}

	fmt.Printf("\nMain Query:\n%s\n", mainQuery)

	// 解析 SQL 语句
	stmt, err = sqlparser.Parse(mainQuery)
	if err != nil {
		fmt.Println("Failed to parse SQL:", err)
		return
	}

	// 输出解析结果
	fmt.Println("Databases, Schemas, Tables and Aliases:")
	switch stmtType := stmt.(type) {
	case *sqlparser.Select:
		ProcessSelectStatement(stmtType)
	case *sqlparser.Union:
		ProcessSelectStatement(stmtType)
	default:
		fmt.Println("Unsupported statement type")
	}
	// var bytes []byte
	// string(bytes)
}

// ProcessTableExpr 解析表表达式，提取库名、模式名、表名和别名
func ProcessTableExpr(tableExpr sqlparser.TableExpr) {
	switch table := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch expr := table.Expr.(type) {
		case sqlparser.TableName:
			databaseName := "None"
			schemaName := "None"

			tableName := expr.Name.String()
			dblist := strings.Split(tableName, ".")
			if len(dblist) == 3 {
				databaseName = dblist[0]
				schemaName = dblist[1]
				tableName = dblist[2]
			} else if len(dblist) == 2 {
				schemaName = dblist[0]
				tableName = dblist[1]
			} else {
				tableName = dblist[0]
			}
			alias := table.As.String()
			if alias == "" {
				alias = tableName
			}

			fmt.Printf("Database: %s, Schema: %s, Table: %s, Alias: %s,expr: %v\n",
				databaseName, schemaName, tableName, alias, expr.String())
		}

	case *sqlparser.JoinTableExpr:
		// 对 JOIN 左右两边的表进行递归处理
		ProcessTableExpr(table.LeftExpr)
		ProcessTableExpr(table.RightExpr)

	case *sqlparser.ParenTableExpr:
		// 对括号中的表表达式进行递归处理
		for _, expr := range table.Exprs {
			ProcessTableExpr(expr)
		}

	default:
		fmt.Println("Unknown table expression")
	}
}

// ProcessSelectStatement 递归解析 SELECT 语句
func ProcessSelectStatement(stmt sqlparser.SelectStatement) {
	switch selectStmt := stmt.(type) {
	case *sqlparser.Select:
		// 处理 FROM 子句
		for _, tableExpr := range selectStmt.From {
			ProcessTableExpr(tableExpr)
		}

		// 递归处理子查询
		for _, selectExpr := range selectStmt.SelectExprs {
			switch expr := selectExpr.(type) {
			case *sqlparser.AliasedExpr:
				if subquery, ok := expr.Expr.(*sqlparser.Subquery); ok {
					ProcessSelectStatement(subquery.Select)
				}
			}
		}

	case *sqlparser.Union:
		// 处理 UNION 左右两边的查询
		ProcessSelectStatement(selectStmt.Left)
		ProcessSelectStatement(selectStmt.Right)

	default:
		fmt.Println("Unknown select statement")
	}
}
