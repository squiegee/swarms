(namespace 'free)
(module dao-hive-gas2 GOVERNANCE
  (defcap GOVERNANCE ()
    true
  )

  (implements gas-payer-v1)
  (use coin)

  (defschema gas
    balance:decimal
    guard:guard)

  (deftable ledger:{gas})

  (defcap GAS_PAYER:bool
    ( user:string
      limit:integer
      price:decimal
    )
    (compose-capability (ALLOW_GAS))
  )

  (defun chain-gas-price ()
    (at 'gas-price (chain-data)))

  (defun chain-gas-limit ()
    (at 'gas-limit (chain-data)))

  (defun enforce-below-or-at-gas-price:bool (gasPrice:decimal)
    (enforce (<= (chain-gas-price) gasPrice)
      (format "Gas Price > {}" [gasPrice])))

  (defun enforce-below-or-at-gas-limit:bool (gasLimit:integer)
    (enforce (<= (chain-gas-limit) gasLimit)
      (format "Gas limit > {}" [gasLimit])))

  (defcap ALLOW_GAS () true)

  (defun create-gas-payer-guard:guard ()
    (create-user-guard (gas-payer-guard))
  )

  (defcap DAO_GAS (msg:object)
    @event true
  )

  (defun gas-payer-guard ()
  (require-capability (GAS))
  (at "miner-keyset" (read-msg)) ;seems to enforce continuous only
  (enforce-below-or-at-gas-price 0.000001)
  (enforce-below-or-at-gas-limit 4000)
  )

)

