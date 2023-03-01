(namespace (read-msg 'ns))

;Contains reference to dapps that integrate with swarms on various chains throughout kadena
;This file is for testing

(module dao-hive-reference GOVERNANCE "Swarms.Finance Reference Manager"

  (defcap GOVERNANCE ()
    @doc "Verifies Contract Governance"
    (enforce-keyset "free.admin-kadena-stake")
  )

  ;///////
  ;REPL
  ;\\\\\\\

  (defun kds-swap-exact-in (swap-in-amount:decimal from-pool-tokenA:module{fungible-v2} to-pool-tokenB:module{fungible-v2} from-pool-account:string to-pool-account:string)
    @doc " KDS SWAP "
    (swap.exchange.swap-exact-in swap-in-amount 0.0 [from-pool-tokenA to-pool-tokenB] from-pool-account to-pool-account (at "guard" (to-pool-tokenB::details to-pool-account)) )
  )

  (defun kds-get-pair (from-pool-tokenA:module{fungible-v2} to-pool-tokenB:module{fungible-v2})
    @doc " KDS GET PAIR "
    (swap.exchange.get-pair from-pool-tokenA to-pool-tokenB)
  )

  (defun kds-get-pair-key (tokenA:module{fungible-v2} tokenB:module{fungible-v2})
    @doc " KDS GET PAIR KEY "
    (swap.exchange.get-pair-key tokenA tokenB)
  )

  (defun kds-add-liquidity (tokenA:module{fungible-v2} tokenB:module{fungible-v2} add-amount-A:decimal add-amount-B:decimal new-treasury-account:string pool-guard:guard)
    @doc " KDS ADD LIQUIDITY "
    (swap.exchange.add-liquidity tokenA tokenB add-amount-A add-amount-B 0.0 0.0 new-treasury-account new-treasury-account pool-guard)
  )

  ; (defun kds-remove-liquidity (tokenA:module{fungible-v2} tokenB:module{fungible-v2} remove-amount:decimal lp-pool-account:string new-treasury-account:string pool-guard:guard)
  ;   @doc " KDS REMOVE LIQUIDITY "
  ;   (swap.exchange.remove-liquidity tokenA tokenB remove-amount 0.0 0.0 lp-pool-account new-treasury-account pool-guard)
  ; )
  (defun kds-remove-liquidity (tokenA:module{fungible-v2} tokenB:module{fungible-v2} remove-amount:decimal lp-pool-account:string new-treasury-account:string pool-guard:guard lp-pool-pair:string swap-account:string remove-amount:decimal)
    @doc " KDS REMOVE LIQUIDITY "
    (install-capability (swap.tokens.TRANSFER lp-pool-pair lp-pool-account swap-account remove-amount))
    (swap.exchange.remove-liquidity tokenA tokenB remove-amount 0.0 0.0 lp-pool-account new-treasury-account pool-guard)
  )

  (defun kds-tokens-get-balance (lp-pool-pair:string lp-pool-account:string)
    @doc " KDS TOKENS GET BALANCE "
    (swap.tokens.get-balance lp-pool-pair lp-pool-account)
  )


  ;///////
  ;CHAIN 0
  ;\\\\\\\

  ; (defun kds-swap-exact-in-c1 (swap-in-amount:decimal from-pool-tokenA:module{fungible-v2} to-pool-tokenB:module{fungible-v2} from-pool-account:string to-pool-account:string)
  ;   @doc " KDS SWAP "
  ;   (kdlaunch.kdswap-exchange.swap-exact-in swap-in-amount 0.0 [from-pool-tokenA to-pool-tokenB] from-pool-account to-pool-account (at "guard" (to-pool-tokenB::details to-pool-account)) )
  ; )
  ;
  ; (defun kds-get-pair-c1 (from-pool-tokenA:module{fungible-v2} to-pool-tokenB:module{fungible-v2})
  ;   @doc " KDS GET PAIR "
  ;   (kdlaunch.kdswap-exchange.get-pair from-pool-tokenA to-pool-tokenB)
  ; )
  ;
  ; (defun kds-get-pair-key-c1 (tokenA:module{fungible-v2} tokenB:module{fungible-v2})
  ;   @doc " KDS GET PAIR KEY "
  ;   (kdlaunch.kdswap-exchange.get-pair-key tokenA tokenB)
  ; )
  ;
  ; (defun kds-add-liquidity-c1 (tokenA:module{fungible-v2} tokenB:module{fungible-v2} add-amount-A:decimal add-amount-B:decimal new-treasury-account:string pool-guard:guard)
  ;   @doc " KDS ADD LIQUIDITY "
  ;   (kdlaunch.kdswap-exchange.add-liquidity tokenA tokenB add-amount-A add-amount-B 0.0 0.0 new-treasury-account new-treasury-account pool-guard)
  ; )
  ;
  ; (defun kds-remove-liquidity-c1 (tokenA:module{fungible-v2} tokenB:module{fungible-v2} remove-amount:decimal lp-pool-account:string new-treasury-account:string pool-guard:guard)
  ;   @doc " KDS REMOVE LIQUIDITY "
  ;   (kdlaunch.kdswap-exchange.remove-liquidity tokenA tokenB remove-amount 0.0 0.0 lp-pool-account new-treasury-account pool-guard)
  ; )
  ; (defun kds-tokens-get-balance (lp-pool-pair:string lp-pool-account:string)
  ;   @doc " KDS TOKENS GET BALANCE "
  ;   (kdlaunch.kdswap-exchange-tokens.get-balance lp-pool-pair lp-pool-account)
  ; )


  ;///////
  ;CHAIN 1
  ;\\\\\\\

  ; (defun kds-swap-exact-in-c1 (swap-in-amount:decimal from-pool-tokenA:module{fungible-v2} to-pool-tokenB:module{fungible-v2} from-pool-account:string to-pool-account:string)
  ;   @doc " KDS SWAP "
  ;   true
  ; )
  ;
  ; (defun kds-get-pair-c1 (from-pool-tokenA:module{fungible-v2} to-pool-tokenB:module{fungible-v2})
  ;   @doc " KDS GET PAIR "
  ;   true
  ; )
  ;
  ; (defun kds-get-pair-key-c1 (tokenA:module{fungible-v2} tokenB:module{fungible-v2})
  ;   @doc " KDS GET PAIR KEY "
  ;   true
  ; )
  ;
  ; (defun kds-add-liquidity-c1 (tokenA:module{fungible-v2} tokenB:module{fungible-v2} add-amount-A:decimal add-amount-B:decimal new-treasury-account:string pool-guard:guard)
  ;   @doc " KDS ADD LIQUIDITY "
  ;   true
  ; )
  ;
  ; (defun kds-remove-liquidity-c1 (tokenA:module{fungible-v2} tokenB:module{fungible-v2} remove-amount:decimal lp-pool-account:string new-treasury-account:string pool-guard:guard)
  ;   @doc " KDS REMOVE LIQUIDITY "
  ;   true
  ; )
  ; (defun kds-tokens-get-balance (lp-pool-pair:string lp-pool-account:string)
  ;   @doc " KDS TOKENS GET BALANCE "
  ;   true
  ; )


)
