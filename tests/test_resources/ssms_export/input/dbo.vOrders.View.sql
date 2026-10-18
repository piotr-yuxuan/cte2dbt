USE [Sales]
GO
/****** Object:  View [dbo].[vOrders]    Script Date: 16/10/2026 09:00:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vOrders] AS
WITH active_customers AS (
    SELECT id, name FROM Customers WHERE active = 1
),
totals (customer_id, total) AS (
    SELECT c.id, SUM(dbo.Orders.amount)
    FROM active_customers AS c
    JOIN dbo.Orders WITH (NOLOCK) ON c.id = dbo.Orders.customer_id
    GROUP BY c.id
)
SELECT * FROM totals
GO
EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPaneCount', @value=1 , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'VIEW',@level1name=N'vOrders'
GO
