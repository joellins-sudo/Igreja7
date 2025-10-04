CREATE INDEX IF NOT EXISTS ix_transactions_sub_congregation_id
  ON public.transactions(sub_congregation_id);
