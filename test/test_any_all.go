package 	test

import (
	"fmt"
	"testing"
	"github.com/tianyuso/tsqlparser/sqlparser"
)

func TestAnyAll(t *testing.T) {
	// 测试ANY/ALL子查询
	testCases := []string{
		// ANY 子查询
		"SELECT * FROM products WHERE price > ANY (SELECT price FROM products WHERE category = 'electronics')",
		"SELECT * FROM products WHERE price < ANY (SELECT price FROM products WHERE category = 'books')",
		"SELECT * FROM products WHERE price = ANY (SELECT price FROM products WHERE category = 'clothing')",

		// ALL 子查询
		"SELECT * FROM products WHERE price > ALL (SELECT price FROM products WHERE category = 'electronics')",
		"SELECT * FROM products WHERE price < ALL (SELECT price FROM products WHERE category = 'books')",
		"SELECT * FROM products WHERE price >= ALL (SELECT price FROM products WHERE category = 'clothing')",

		// 复杂的ANY/ALL子查询
		"SELECT * FROM orders WHERE amount > ANY (SELECT AVG(amount) FROM orders GROUP BY user_id)",
		"SELECT * FROM users WHERE age < ALL (SELECT age FROM users WHERE status = 'admin')",
	}

	for i, sql := range testCases {
		fmt.Printf("\n=== ANY/ALL Test Case %d ===\n", i+1)
		fmt.Printf("SQL: %s\n", sql)

		stmt, err := sqlparser.Parse(sql)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("Success: Parsed successfully\n")
			fmt.Printf("Type: %T\n", stmt)
			// 重新格式化输出，验证解析正确性
			formatted := sqlparser.String(stmt)
			fmt.Printf("Formatted: %s\n", formatted)
		}
	}
}
