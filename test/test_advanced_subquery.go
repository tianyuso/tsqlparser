package test

import (
	"fmt"
	"testing"
	"github.com/tianyuso/tsqlparser/sqlparser"
)

func TestAdvancedSubquery(t *testing.T) {
	// 更复杂的测试用例
	testCases := []string{
		// 标量子查询
		"SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count FROM users",

		// CASE 中的子查询
		"SELECT name, CASE WHEN id IN (SELECT user_id FROM premium_users) THEN 'Premium' ELSE 'Regular' END as status FROM users",

		// 多个子查询
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) AND id NOT IN (SELECT user_id FROM banned_users)",

		// 子查询中的 UNION
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders UNION SELECT user_id FROM reviews)",

		// 相关子查询
		"SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100)",

		// ANY/ALL 子查询 (如果支持)
		"SELECT * FROM products WHERE price > ANY (SELECT price FROM products WHERE category = 'electronics')",

		// 子查询中的聚合函数
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders GROUP BY user_id HAVING COUNT(*) > 5)",

		// 多层嵌套
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE category IN (SELECT name FROM categories WHERE active = 1)))",

		// WITH 子句 (CTE)
		"WITH user_orders AS (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id) SELECT u.name, uo.order_count FROM users u JOIN user_orders uo ON u.id = uo.user_id",

		// 子查询作为 JOIN 的一部分
		"SELECT u.name, o.total FROM users u JOIN (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id) o ON u.id = o.user_id",
	}

	for i, sql := range testCases {
		fmt.Printf("\n=== Advanced Test Case %d ===\n", i+1)
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
