package test

import (
	"fmt"
	"testing"

	"github.com/tianyuso/tsqlparser/sqlparser"
)

func TestSubquery(t *testing.T) {
	// 测试用例
	testCases := []string{
		// EXISTS 子查询
		"SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",

		// IN 子查询
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)",

		// NOT IN 子查询
		"SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM blacklist)",

		// 子查询作为表
		"SELECT u.name FROM (SELECT * FROM users WHERE age > 18) u",

		// 复杂的嵌套子查询
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE category = 'electronics'))",

		// UPDATE 中的子查询
		"UPDATE users SET status = 'premium' WHERE id IN (SELECT user_id FROM orders GROUP BY user_id HAVING SUM(amount) > 1000)",

		// DELETE 中的子查询
		"DELETE FROM users WHERE id IN (SELECT user_id FROM inactive_users)",

		// INSERT 中的子查询
		"INSERT INTO user_summary (user_id, total_orders) SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
	}

	for i, sql := range testCases {
		fmt.Printf("\n=== Test Case %d ===\n", i+1)
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
