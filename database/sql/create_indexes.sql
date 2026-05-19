CREATE INDEX idx_transaction_date
ON public.transactions(trans_date_trans_time);

CREATE INDEX idx_cc_num
ON public.transactions(cc_num);

CREATE INDEX idx_is_fraud
ON public.transactions(is_fraud);

CREATE INDEX idx_category
ON public.transactions(category);

CREATE INDEX idx_trans_num
ON public.transactions(trans_num);