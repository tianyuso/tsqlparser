package test

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

// 全局变量存储表信息
var tableInfos []map[string]string

// ParseAndProcessSQL 解析SQL语句并处理表信息
func ParseAndProcessSQL(query string) error {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		fmt.Println("Failed to parse SQL:", err)
		return err
	}

	// 解析表信息
	fmt.Printf("stmt: %+v\n", stmt)
	switch stmtType := stmt.(type) {
	case *sqlparser.Select:
		ProcessSelectStatement(stmtType)
	case *sqlparser.Union:
		ProcessSelectStatement(stmtType)

	default:
		fmt.Println("Unsupported statement type")
	}
	return nil
}

// CheckTable 解析SQL语句并返回表信息
func CheckTable(sql string) []map[string]string {
	// 初始化表信息切片
	tableInfos = make([]map[string]string, 0)

	ctes, mainQuery, isCte, err := ParseCTEQuery(sql)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return tableInfos
	}

	fmt.Printf("isCTEs: %v,ctes: %v\n", isCte, ctes)
	if isCte {
		for _, cte := range ctes {
			fmt.Printf("\nCTE Name: %s\nCTE Query:\n%s\n", cte.Name, cte.Query)
			ParseAndProcessSQL(cte.Query)
		}
		fmt.Printf("\nMain Query:%s\n", mainQuery)
		// 解析主查询的表信息
		err = ParseAndProcessSQL(mainQuery)
		if err != nil {
			return tableInfos
		}
	} else {
		mainQuery = sql
		fmt.Printf("\n sql is not cte ,Query:%s\n", mainQuery)
		// 解析主查询的表信息
		err = ParseAndProcessSQL(mainQuery)
		if err != nil {
			return tableInfos
		}
	}

	// 输出收集到的所有表信息
	fmt.Println("\n=== 解析结果 ===")
	fmt.Printf("共找到 %d 个表:\n", len(tableInfos))
	for i, tableInfo := range tableInfos {
		fmt.Printf("表 %d: Database=%s, Schema=%s, Table=%s, Alias=%s\n",
			i+1, tableInfo["database"], tableInfo["schema"], tableInfo["table"], tableInfo["alias"])
	}

	return tableInfos
}

func ParseCTEQuery(sql string) (ctes []CTEInfo, mainQuery string, isCte bool, err error) {
	var currentPos, startPos int
	var currentCTE string

	tableInfos = make([]map[string]string, 0)
	sql = strings.TrimSpace(sql)
	// Simplified regex pattern for CTEs
	ctePattern := regexp.MustCompile(`(?is)([a-zA-Z0-9_]+)\s+AS\s*\(\s*(.*)\s*\)`)

	// Regular expression to split the WITH clause from the main query
	withPattern := regexp.MustCompile(`(?is)\s*WITH\s+(.*)\s*\)\s*(SELECT\s+.*)$`)

	matches := withPattern.FindStringSubmatch(sql)
	Withmatched, _ := regexp.MatchString("^with", sql)
	fmt.Printf("Withmatched: %v,len: %d\n", Withmatched, len(matches))
	if Withmatched && len(matches) > 0 {
		isCte = true
	}

	fmt.Printf("startPos: %d\n", startPos)
	// First, normalize the SQL string
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(strings.TrimSpace(sql), " ")

	// Match the overall WITH structure
	for i, match := range matches {
		fmt.Printf("match %d: %s \n", i, match)
	}

	if isCte {
		// Extract CTEs
		cteSection := matches[1]
		depth := 0
		cteSection = cteSection + ")"
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

					cteMatch := ctePattern.FindStringSubmatch(currentCTE)
					fmt.Printf("currentCTE: %s,cteMatch: %v\n", currentCTE, cteMatch)
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
	} else {
		mainQuery = sql
	}

	return ctes, mainQuery, isCte, nil
}

func TestMain() {
	// sql := `
	// with
	// c1 as
	// (
	//     select * from db1.dbo.table1  t1
	//     inner join dbo.xx2 as x2 on x2.id= t1.xx2_id
	//     where x2.name like 'abc%' and t1.id in (select id from dbo.table2
	// 	where id > 20)
	// ),
	// ct2 as
	// (
	//     select * from dbo.table2 where id > 20
	// ),
	// cte3 as
	// (
	//     select * from db44..table3
	//     left join table33  on table3.id = table33.t3_id
	//     where price < 100
	// )
	// select a.id,b.name,c.order_id from c1 a, ct2 b, cte3 c where a.id = b.id and a.id = c.id
	// union
	// SELECT u.id, u.name, o.order_id
	// FROM database1.schema1.users u
	// 	LEFT JOIN schema2.orders o ON u.id = o.user_id
	// WHERE u.age > 18
	// 	AND u.id IN (
	// 		SELECT user_id
	// 		FROM database2.schema2.transactions
	// 		WHERE amount > 100
	// 	)
	// UNION
	// SELECT a.id, a.name, b.order_id
	// FROM db2.schema3.account a
	// 	INNER JOIN schema4.billing b ON a.id = b.account_id
	// `
	// sql := `
	// select * from db1.dbo.table1  t1
	//     inner join dbo.xx2 as x2 on x2.id= t1.xx2_id
	//     where x2.name like 'abc%' and t1.id in (select id from dbo.table2
	// 	where id > 20)
	// // `
	// sql := `
	// select * from db1.dbo.table1  t1
	//     inner join dbo.xx2 as x2 on x2.id= t1.xx2_id
	//     where x2.name like 'abc%' and exists (select id from dbo.table2
	// 	where id > 20 and id = t1.id)
	// `
	sql := `
	select * from db1.dbo.table1  t1
	    inner join dbo.xx2 as x2 on x2.id= t1.xx2_id
	    where x2.name like 'abc%' and exists (select id from dbo.table2
		where id > 20 and id = t1.id)
	`
	// 调用CheckTable函数解析SQL并获取表信息
	result := CheckTable(sql)

	// 可以进一步处理返回的结果
	fmt.Printf("\n返回的表信息切片长度: %d\n", len(result))

	fmt.Println("Done")
}

// ProcessTableExpr 解析表表达式，提取库名、模式名、表名和别名
func ProcessTableExpr(tableExpr sqlparser.TableExpr) {
	switch table := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch expr := table.Expr.(type) {
		case sqlparser.TableName:
			databaseName := "None"
			schemaName := "None"

			// 处理完整的表名（可能包含数据库和模式）
			if !expr.Qualifier.IsEmpty() {
				if !expr.Schema.IsEmpty() {
					// database.schema.table 格式
					databaseName = expr.Qualifier.String()
					schemaName = expr.Schema.String()
				} else {
					// database.table 或 schema.table 格式
					schemaName = expr.Qualifier.String()
				}
			}

			tableName := expr.Name.String()
			alias := table.As.String()
			if alias == "" {
				alias = tableName
			}

			// 将表信息添加到切片中
			tableInfo := map[string]string{
				"database": databaseName,
				"schema":   schemaName,
				"table":    tableName,
				"alias":    alias,
			}
			tableInfos = append(tableInfos, tableInfo)
		case *sqlparser.Subquery:
			// 处理子查询作为表的情况
			ProcessSelectStatement(expr.Select)
		}

	case *sqlparser.JoinTableExpr:
		// 对 JOIN 左右两边的表进行递归处理
		ProcessTableExpr(table.LeftExpr)
		ProcessTableExpr(table.RightExpr)

		// 处理 JOIN 条件中的子查询
		if table.On != nil {
			ProcessExpr(table.On)
		}

	case *sqlparser.ParenTableExpr:
		// 对括号中的表表达式进行递归处理
		for _, expr := range table.Exprs {
			ProcessTableExpr(expr)
		}

	default:
		fmt.Println("Unknown table expression")
	}
}

// ProcessExpr 递归处理表达式中的子查询
func ProcessExpr(expr sqlparser.Expr) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *sqlparser.Subquery:
		// 处理子查询
		ProcessSelectStatement(e.Select)
	case *sqlparser.ComparisonExpr:
		// 处理比较表达式（如 IN, EXISTS 等）
		ProcessExpr(e.Left)
		ProcessExpr(e.Right)
	case *sqlparser.ExistsExpr:
		// 处理 EXISTS 子查询
		if e.Subquery != nil {
			ProcessSelectStatement(e.Subquery.Select)
		}
	case *sqlparser.AndExpr:
		// 处理 AND 表达式
		ProcessExpr(e.Left)
		ProcessExpr(e.Right)
	case *sqlparser.OrExpr:
		// 处理 OR 表达式
		ProcessExpr(e.Left)
		ProcessExpr(e.Right)
	case *sqlparser.NotExpr:
		// 处理 NOT 表达式
		ProcessExpr(e.Expr)
	case *sqlparser.ParenExpr:
		// 处理括号表达式
		ProcessExpr(e.Expr)
	case *sqlparser.RangeCond:
		// 处理 BETWEEN 条件
		ProcessExpr(e.Left)
		ProcessExpr(e.From)
		ProcessExpr(e.To)
	case *sqlparser.IsExpr:
		// 处理 IS 表达式
		ProcessExpr(e.Expr)
	case *sqlparser.FuncExpr:
		// 处理函数表达式中的子查询
		for _, selectExpr := range e.Exprs {
			if aliasedExpr, ok := selectExpr.(*sqlparser.AliasedExpr); ok {
				ProcessExpr(aliasedExpr.Expr)
			}
		}
	case *sqlparser.CaseExpr:
		// 处理 CASE 表达式
		ProcessExpr(e.Expr)
		for _, when := range e.Whens {
			ProcessExpr(when.Cond)
			ProcessExpr(when.Val)
		}
		ProcessExpr(e.Else)
	case sqlparser.ValTuple:
		// 处理值元组中的表达式
		for _, val := range e {
			ProcessExpr(val)
		}
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

		// 处理 SELECT 表达式中的子查询
		for _, selectExpr := range selectStmt.SelectExprs {
			switch expr := selectExpr.(type) {
			case *sqlparser.AliasedExpr:
				ProcessExpr(expr.Expr)
			}
		}

		// 处理 WHERE 子句中的子查询
		if selectStmt.Where != nil {
			ProcessExpr(selectStmt.Where.Expr)
		}

		// 处理 HAVING 子句中的子查询
		if selectStmt.Having != nil {
			ProcessExpr(selectStmt.Having.Expr)
		}

		// 处理 GROUP BY 中的子查询
		for _, groupExpr := range selectStmt.GroupBy {
			ProcessExpr(groupExpr)
		}

		// 处理 ORDER BY 中的子查询
		for _, orderExpr := range selectStmt.OrderBy {
			ProcessExpr(orderExpr.Expr)
		}

	case *sqlparser.Union:
		// 处理 UNION 左右两边的查询
		ProcessSelectStatement(selectStmt.Left)
		ProcessSelectStatement(selectStmt.Right)

	case *sqlparser.ParenSelect:
		// 处理括号中的 SELECT 语句
		ProcessSelectStatement(selectStmt.Select)

	default:
		fmt.Println("Unknown select statement")
	}
}
