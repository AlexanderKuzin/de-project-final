with tran as (
	SELECT 
		operation_id as id,
		t.account_number_from as account,
		t.currency_code,
		case when t.status = 'done' then t.amount
			when t.status = 'chargeback' then t.amount * (-1) 
		end amount,
		t.transaction_dt::DATE as tran_date
	from STV2024080615__STAGING.transactions t 
	where t.status in ('done', 'chargeback')
		and t.transaction_type not in ('authorization', 'authorization_commission', 'loyalty_cashback')
		and account_number_from > 0
		and account_number_to > 0
		and tran_date = '{0}'
),
conv_rate as (
	select 
		c.currency_code,
		c.currency_code_with,
		c.currency_with_div,
		c.date_update 
	from STV2024080615__STAGING.currencies c  
	where c.date_update::DATE = '{0}' 
		and c.currency_code_with = 420)
SELECT 
	a.date_update,
	a.currency_from,
	sum(a.usd_amount) as amount_total,
	count(distinct a.id) as cnt_transactions,
	COUNT(DISTINCT a.id) / COUNT(DISTINCT a.account) AS avg_transactions_per_account,
	count(distinct a.account) as cnt_accounts_make_transactions
from (
	SELECT 
		t.id,
		t.account,
		t.currency_code as currency_from,
		t.amount,
		t.amount * coalesce(cr.currency_with_div, 1) as usd_amount,
		t.tran_date as date_update
	from tran t
		left join conv_rate cr on cr.date_update = t.tran_date
			and cr.currency_code = t.currency_code
)  a 
group by a.date_update, a.currency_from;
