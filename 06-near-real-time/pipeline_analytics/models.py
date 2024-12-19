import bauplan

"""

    This model joins the purchase_sessions and ecommerce_clean tables 
	on the user_session and event_hour columns, and adds some 
	aggregations to the data.

"""


@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model(materialization_strategy="REPLACE")
def ecommerce_metrics_base(
	purchase_sessions=bauplan.Model('purchase_sessions'),
 	ecommerce_clean=bauplan.Model('ecommerce_clean')
):	
	import duckdb
	con = duckdb.connect()
	q = """

            SELECT
				e.event_hour,
				e.brand,
				SUM(CASE WHEN e.event_type = 'view' THEN 1 ELSE 0 END) AS views,
				COUNT(DISTINCT e.user_session) AS unique_sessions,
				COUNT(e.user_session) AS total_sessions,
				COUNT(DISTINCT p.purchase_session) AS orders,
				SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased_products,
				SUM(CASE WHEN e.event_type = 'purchase' THEN e.price ELSE 0 END) AS revenue,
            FROM ecommerce_clean as e
            LEFT JOIN purchase_sessions as p
				ON e.user_session = p.purchase_session 
				AND e.event_hour = p.event_hour
            GROUP BY 1,2
			;

  	"""
	data = con.execute(q).arrow()
	
	return data