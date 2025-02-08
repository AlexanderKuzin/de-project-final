DROP TABLE IF EXISTS STV2024080615__STAGING.currencies;

CREATE TABLE STV2024080615__STAGING.currencies (
	currency_code int NOT NULL,
	currency_code_with int NOT NULL,
	date_update timestamp NOT NULL,
	currency_with_div numeric(5, 3) NOT NULL
)
ORDER BY date_update;


DROP TABLE IF EXISTS STV2024080615__STAGING.transactions;

CREATE TABLE STV2024080615__STAGING.transactions (
	operation_id varchar(60) NOT NULL,
	account_number_from int NOT NULL,
	account_number_to int NOT NULL,
	currency_code int NOT NULL,
	country varchar(30) NOT NULL,
	status varchar(30) NOT NULL,
	transaction_type varchar(30) NOT NULL,
	amount int NOT NULL,
	transaction_dt timestamp NOT NULL
)
ORDER BY transaction_dt
SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES;

------

DROP TABLE IF EXISTS STV2024080615__DWH.global_metrics;

CREATE TABLE STV2024080615__DWH.global_metrics (
	date_update timestamp NOT NULL,
	currency_from int NOT NULL,
	amount_total numeric(15, 3) NOT NULL,
	cnt_transactions int NOT NULL,
	avg_transactions_per_account float NOT NULL,
	cnt_accounts_make_transactions int NOT NULL
)
ORDER BY date_update;