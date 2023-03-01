(namespace (read-msg 'ns))

(module dao-hive-factory GOVERNANCE "Swarms.Finance DAO Hive Factory"
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;           Swarms.Finance           ;
;                 __                 ;
;      DAO     __/  \__    Factory   ;
;      __     /  \__/  \     __      ;
;     / \\    \__/  \__/    // \     ;
;  \\ \_//    /  \__/  \    \\_/ //  ;
;  (')(||)-   \__/  \__/   -(||)(')  ;
;     '''        \__/        '''     ;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;Creates and manages Kadena DAO Hives;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

    ;;;;; CONSTANTS
    (defconst ACCOUNT_ID_CHARSET CHARSET_LATIN1
    " Allowed character set for Account IDs. ")

    (defconst NAME_MIN_LENGTH 3
      " Minimum character length for names. ")

    (defconst NAME_MAX_LENGTH 40
      " Maximum character length for names. ")

    (defconst DESCRIPTION_MIN_LENGTH 3
      " Minimum character length for names. ")

    (defconst DESCRIPTION_MAX_LENGTH 400
      " Maximum character length for names. ")

    ;;;;; CAPABILITIES

    (defcap GOVERNANCE ()
      @doc "Verifies Contract Governance"
      (enforce-keyset "free.admin-kadena-stake")
    )

    (defcap ACCOUNT_GUARD(account:string)
        @doc "Verifies Account Existence"
        (enforce-guard
            (at "guard" (coin.details account))
        )
    )

    (defcap CREATOR_GUARD(dao-id:string)
        @doc "Verifies Hive Creator Account"
        (let
                (
                    (dao-data (read daos-table dao-id ["dao_creator"]))
                )
                (enforce-guard
                    (at "guard" (coin.details (at "dao_creator" dao-data) ))
                )
        )
    )

    (defcap MEMBERS_GUARD(dao-id:string account:string)
        @doc "Verifies account belongs to a treasurer"
        (let*
                (
                    (memberships (read dao-membership-ids-table account))
                    (membership-ids (at "dao_ids" memberships))
                )
                (enforce (= (contains dao-id membership-ids) true) "Access not granted")
        )
    )

    ;For adding accounts
    (defcap ADD_ACCOUNT
      (dao_id:string)
      true
    )
    ;For adding accounts
    (defcap CAN_ADD
      (dao_id:string)
          (compose-capability (ADD_ACCOUNT dao_id))
    )
    ;For updating dao update messages
    (defcap ADD_UPDATE
      (dao_id:string)
      true
    )
    ;For updating dao update messages
    (defcap CAN_UPDATE
      (dao_id:string)
      (compose-capability (ADD_UPDATE dao_id))
    )
    ;For creating voting options
    (defcap CAN_ADD_OPTION
      (proposal_id:string)
          (compose-capability (ADD_OPTION proposal_id))
    )
    ;For creating voting options
    (defcap ADD_OPTION
      (proposal_id:string)
      true
    )
    ;For completing dao actions
    (defcap COMPLETE_ACTION
      ()
      true
    )
    ;For completing dao actions
    (defcap CAN_COMPLETE
      (proposal_id:string dao_id:string)
          (let*
              (
                (proposal-data (read dao-proposals-table proposal_id))
                (proposal-dao (at "proposal_dao_id" proposal-data))
                (proposal-completed (at "proposal_completed" proposal-data))
              )
              (enforce (= dao_id proposal-dao) "This proposal belongs to a different Hive")
              (enforce (= proposal-completed false) "This proposal has already been completed")
              (compose-capability (COMPLETE_ACTION))
          )
    )

    ;DAO guards
    (defcap PRIVATE_RESERVE
          (id:string account:string)
        true)

    (defun enforce-private-reserve:bool
        (id:string account:string)
      (require-capability (PRIVATE_RESERVE id account)))

    (defun create-pool-guard:guard
        (id:string account:string)
      (create-user-guard (enforce-private-reserve id account)))

    (defun get-account-principal (id:string account:string)
     (create-principal (create-pool-guard id account))
    )

    ;;;;;;;;;; EVENTS ;;;;;;;;;;;;;;;;;;;;;;;;;;

    (defcap DAO_HIVE_UPDATED (dao_id:string update_time:time)
      @doc " Emitted when a DAO Hive is updated "
      @event true
    )

    ;;;;;;;;;; SCHEMAS AND TABLES ;;;;;;;;;;;;;;

    (defschema dao-schema
      @doc " DAO Hive schema "
      dao_id:string
      dao_name:string
      dao_creator:string
      dao_image:string
      dao_long_description:string
      dao_pool_count:integer
      dao_proposal_count:integer
      dao_messages_count:integer
      dao_members_count:integer
      dao_updates_count:integer
      dao_daily_proposal_limit:integer
      dao_threshold:decimal
      dao_voter_threshold:decimal
      dao_minimum_proposal_time:decimal
      dao_members_locked:bool
      dao_total_weight:decimal
      dao_use_weight:bool
      dao_all_can_propose:bool
      dao_chain:string
      dao_active_chains:[string]
    )

    (defschema pool-record-schema
      @doc " Pool record schema "
      pool_id:string
      pool_name:string
      pool_use_weight:bool
      pool_weight:decimal
      pool_description:string
      pool_account:string
      pool_token:module{fungible-v2}
      pool_tokenB:module{fungible-v2}
      pool_lp:bool
      pool_pair:string
      pool_chain:string
      pool_auto:bool
    )

    (defschema account-schema
      @doc " Account schema "
      account_id:string
      account_name:string
      account_dao_id:string
      account_banned:bool
      account_weight:decimal
      account_can_propose:bool
      account_count:integer
    )

    (defschema pool-action-schema
      @doc " Pool action schema "
      action:string
      action_strings:[string]
      action_integers:[integer]
      action_decimals:[decimal]
    )

    (defschema pool-proposal-schema
      @doc " Pool proposal schema "
      proposal_id:string
      proposal_count:integer
      proposal_title:string
      proposal_description:string
      proposal_start_time:time
      proposal_end_time:time
      proposal_completed_time:time
      proposal_completed_action:string
      proposal_completed_consensus:decimal
      proposal_completed_voter_consensus:decimal
      proposal_dao_id:string
      proposal_options_count:integer
      proposal_creator:string
      proposal_completed:bool
      proposal_chain:string
    )

    (defschema vote-schema
      @doc " Stores votes "
      vote_count:integer
      vote_option:object{pool-action-schema}
      vote_description:string
      vote_weight:decimal
    )

    (defschema dao-membership-schema
      @doc " Stores account/dao memberships "
      dao_ids:[string]
    )

    ;Message schema
    (defschema dao-message-schema
      @doc " Message schema "
      message_from:string
      message_date:time
      message:string
      message_title:string
    )

    ;User Vote Record Schema
    (defschema user-vote-record
      @doc " Vote record schema "
      vote_account:string
      vote_proposal:string
      vote_time:time
      vote_option:integer
    )

    ;User Proposition Record Schema
    (defschema user-proposition-record
      @doc " Proposition record schema "
      pr_account:string
      pr_date:string
      pr_proposition_count:integer
      pr_propositions:[string]
    )

    ;;;;;;;;;;TABLES;;;;;;;;;;

    (deftable daos-table:{dao-schema})
    (deftable dao-membership-ids-table:{dao-membership-schema})
    (deftable dao-messages-table:{dao-message-schema})
    (deftable dao-updates-table:{dao-message-schema})
    (deftable dao-accounts-table:{account-schema})
    (deftable dao-pools-table:{pool-record-schema})
    (deftable dao-proposals-table:{pool-proposal-schema})
    (deftable dao-votes-table:{vote-schema})
    (deftable user-vote-records:{user-vote-record})
    (deftable user-proposition-records:{user-proposition-record})
    (deftable dao-accounts-count-table:{account-schema})


    ;//DAO COPY

    (defschema dao-schema-xchain
      @doc " DAO Multichain Copy schema "
      dao_id:string
      dao_name:string
      dao_creator:string
      dao_image:string
      dao_long_description:string
      dao_members_count:integer
      dao_daily_proposal_limit:integer
      dao_threshold:decimal
      dao_voter_threshold:decimal
      dao_minimum_proposal_time:decimal
      dao_members_locked:bool
      dao_total_weight:decimal
      dao_use_weight:bool
      dao_all_can_propose:bool
      dao_members:[object{account-schema}]
    )

    (defpact copy-dao-crosschain:string
    ( account_id:string dao_id:string target-chain:string )
    @doc "Copy DAO to other chain"
    (step
      (with-capability (ACCOUNT_GUARD account_id)
        (with-capability (MEMBERS_GUARD dao_id account_id)

          (enforce (!= "" target-chain) "empty target-chain")
          (enforce (!= (at 'chain-id (chain-data)) target-chain)
            "cannot run cross-chain transfers to the same chain")

          (let*
              (
                  (dao-data (read daos-table dao_id))
                  (dao-locked:bool (at "dao_members_locked" dao-data))
              )
              ;Log new chain
              (with-default-read daos-table dao_id
                { "dao_active_chains" : [] }
                { "dao_active_chains" := t-dao_active_chains}
                (if (= (contains target-chain t-dao_active_chains) false)
                  (update daos-table dao_id
                    {
                        "dao_active_chains": (+ [target-chain] t-dao_active_chains )
                    }
                  )
                  true
                )
              )

              (let
                ((crosschain-details:object{dao-schema-xchain}
                  { "dao_id"       : (at "dao_id" dao-data)
                  , "dao_name" : (at "dao_name" dao-data)
                  , "dao_image"         : (at "dao_image" dao-data)
                  , "dao_long_description"   : (at "dao_long_description" dao-data)
                  , "dao_members_count"         : (at "dao_members_count" dao-data)
                  , "dao_daily_proposal_limit"         : (at "dao_daily_proposal_limit" dao-data)
                  , "dao_threshold"         : (at "dao_threshold" dao-data)
                  , "dao_voter_threshold"         : (at "dao_voter_threshold" dao-data)
                  , "dao_minimum_proposal_time"         : (at "dao_minimum_proposal_time" dao-data)
                  , "dao_members_locked"         : (at "dao_members_locked" dao-data)
                  , "dao_total_weight"         : (at "dao_total_weight" dao-data)
                  , "dao_use_weight"         : (at "dao_use_weight" dao-data)
                  , "dao_all_can_propose"         : (at "dao_all_can_propose" dao-data)
                  , "dao_creator"         : (at "dao_creator" dao-data)
                  , "dao_members"         : (get-all-dao-members dao_id)
                  }
                ))
                (yield crosschain-details target-chain)
              )
           )
        )
      )
    )

    (step
      (resume
        { "dao_id"       := c_dao_id
        , "dao_name" := c_dao_name
        , "dao_image"         := c_dao_image
        , "dao_long_description"   := c_dao_long_description
        , "dao_members_count"         := c_dao_members_count
        , "dao_daily_proposal_limit"         := c_dao_daily_proposal_limit
        , "dao_threshold"         := c_dao_threshold
        , "dao_voter_threshold"         := c_dao_voter_threshold
        , "dao_minimum_proposal_time"         := c_dao_minimum_proposal_time
        , "dao_members_locked"         := c_dao_members_locked
        , "dao_total_weight"         := c_dao_total_weight
        , "dao_use_weight"         := c_dao_use_weight
        , "dao_all_can_propose"         := c_dao_all_can_propose
        , "dao_creator"         := c_dao_creator
        , "dao_members"         := c_dao_members
        }

        (with-default-read daos-table c_dao_id
          { "dao_pool_count" : 0, "dao_proposal_count" : 0, "dao_updates_count" : 0, "dao_messages_count" : 0 }
          { "dao_pool_count" := c_dao_pool_count, "dao_proposal_count" := c_dao_proposal_count, "dao_updates_count" := c_dao_updates_count, "dao_messages_count" := c_dao_messages_count }
          (write daos-table c_dao_id
            {
                "dao_id": c_dao_id,
                "dao_name": c_dao_name,
                "dao_creator": c_dao_creator,
                "dao_image": c_dao_image,
                "dao_long_description": c_dao_long_description,
                "dao_members_count": c_dao_members_count,
                "dao_threshold": c_dao_threshold,
                "dao_voter_threshold": c_dao_voter_threshold,
                "dao_daily_proposal_limit": c_dao_daily_proposal_limit,
                "dao_pool_count": c_dao_pool_count,
                "dao_proposal_count": c_dao_proposal_count,
                "dao_updates_count": c_dao_updates_count,
                "dao_messages_count": c_dao_messages_count,
                "dao_minimum_proposal_time": c_dao_minimum_proposal_time,
                "dao_members_locked": c_dao_members_locked,
                "dao_total_weight": c_dao_total_weight,
                "dao_use_weight": c_dao_use_weight,
                "dao_all_can_propose": c_dao_all_can_propose,
                "dao_chain": (at "chain-id" (chain-data)),
                "dao_active_chains": [(at "chain-id" (chain-data))]
            }
          )
        )

        ;Add new members to dao
        (with-capability (CAN_ADD c_dao_id)
          (map (mass-copy) c_dao_members)
        )

      )
    )
  )


  (defun mass-copy (new_account:object{account-schema})
    @doc "Multichain copy helper function"
      (bind new_account {
                          "account_id" := account_id,
                          "account_name" := account_name,
                          "account_dao_id" := account_dao_id,
                          "account_banned" := account_banned,
                          "account_weight" := account_weight,
                          "account_can_propose" := account_can_propose,
                          "account_count" := account_count
                        }
                        (require-capability (ADD_ACCOUNT account_dao_id))
                        (with-default-read dao-membership-ids-table account_id
                          { "dao_ids" : [] }
                          { "dao_ids" := t-member-ids}
                          (if (= (contains account_dao_id t-member-ids) false)
                            (write dao-membership-ids-table account_id
                              {
                                  "dao_ids": (+ [account_dao_id] t-member-ids )
                              }
                            )
                            true
                          )
                        )
                        (write dao-accounts-table (get-user-key account_id account_dao_id)
                          {
                              "account_id": account_id,
                              "account_name": account_name,
                              "account_dao_id": account_dao_id,
                              "account_banned": account_banned,
                              "account_weight": account_weight,
                              "account_can_propose": account_can_propose,
                              "account_count": account_count
                          }
                        )
                        (write dao-accounts-count-table (get-2-key account_count account_dao_id)
                          {
                              "account_id": account_id,
                              "account_name": account_name,
                              "account_dao_id": account_dao_id,
                              "account_banned": account_banned,
                              "account_weight": account_weight,
                              "account_can_propose": account_can_propose,
                              "account_count": account_count
                          }
                        )
      )
    )

    ;;///////////////////////
    ;;DAO CREATION
    ;;//////////////////////

    ;Creates a new DAO Hive
    ;name: dao name (3-40 characters), string, ex "Test DAO"
    ;image: link to dao icon/image, string, ex "https://link"
    ;long_description: long description of dao, string (3-400 chars), ex "My awesome dao"
    ;min_proposal_time: minimum time in seconds proposals must run, decimal, ex 86400.00
    ;threshold: % of voters required to pass a vote, decimal < 1.0, 1.0 = 100%, ex 0.5
    ;members: list of member ids, add-account-schema, ex: [{"id":"k:stuart"}]
    ;locked: lock this dao from the start, a locked dao requires voting to edit the dao, bool, ex: false
    ;all_propose: can everyone propose or only the creator? ex: true
    ;use_weights: enable weight mode where voting power is based on deposits to the dao, ex: false

    (defun create-dao
      (
        name:string
        creator:string
        image:string
        long_description:string
        min_proposal_time:decimal
        threshold:decimal
        voter_threshold:decimal
        members:[object:{add-account-schema-create}]
        locked:bool
        all_propose:bool
        use_weights:bool
        )
        @doc "Creates a new DAO Hive"
        (with-capability (ACCOUNT_GUARD creator)
            ;Enforce rules
            (enforce-valid-name name)
            (enforce-valid-description long_description)
            (enforce (>= min_proposal_time 0.0) "Positive Minimum Proposal Time Only")
            (enforce (<= threshold 1.0) "Threshold must be <= 1.0")
            (enforce (>= threshold 0.0) "Positive Threshold Only")
            (enforce (<= voter_threshold 1.0) "Threshold must be <= 1.0")
            (enforce (>= voter_threshold 0.0) "Positive Threshold Only")
            (enforce (>= (length members) 1) "Must add alteast 1 member when creating a DAO")
            (let
                (
                  (dao_id:string (create-account-key name creator))
                )

                ;Insert DAO
                (insert daos-table dao_id
                    {
                        "dao_id": dao_id,
                        "dao_name": name,
                        "dao_creator": creator,
                        "dao_image": image,
                        "dao_long_description": long_description,
                        "dao_members_count": 0,
                        "dao_threshold": threshold,
                        "dao_voter_threshold": voter_threshold,
                        "dao_daily_proposal_limit": 5,
                        "dao_pool_count": 0,
                        "dao_proposal_count": 0,
                        "dao_updates_count": 1,
                        "dao_messages_count": 1,
                        "dao_minimum_proposal_time": min_proposal_time,
                        "dao_members_locked": locked,
                        "dao_total_weight": 0.0,
                        "dao_use_weight": use_weights,
                        "dao_all_can_propose": all_propose,
                        "dao_chain": (at "chain-id" (chain-data)),
                        "dao_active_chains": [(at "chain-id" (chain-data))]
                    }
                )

                ;Insert new dao update
                (insert dao-updates-table (get-2-key 1 dao_id)
                    {
                        "message_from": "Hive",
                        "message_date": (at "block-time" (chain-data)),
                        "message_title": "Genesis",
                        "message": (format "Created Hive with ID {}" [dao_id])
                    }
                )

                ;Insert new dao messages
                (insert dao-messages-table (get-2-key 1 dao_id)
                    {
                        "message_from": "Hive",
                        "message_date": (at "block-time" (chain-data)),
                        "message_title": "Genesis",
                        "message": (format "Created Hive with ID {}" [dao_id])
                    }
                )

                ;Add new members to dao
                (with-capability (CAN_ADD dao_id)
                  (map (mass-adder dao_id) members )
                )

                ;Update creator with permissions
                (update dao-accounts-table (get-user-key creator dao_id)
                  {
                      "account_can_propose": true
                  }
                )

                (update dao-accounts-count-table (get-2-key 0 dao_id)
                  {
                      "account_can_propose": true
                  }
                )

                ;Return a message
                (format "Created DAO Hive: {}" [dao_id])

            )

        )
    )

    ;;Locks a dao so it's not editable by the dao creator
    (defun lock-dao
      (account_id:string dao_id:string)
        @doc " Locks a DAO's member list "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (CREATOR_GUARD dao_id)
            (update daos-table dao_id
              {
                  "dao_members_locked": true
              }
            )
            (with-capability (CAN_UPDATE dao_id)
              (add-dao-update dao_id account_id "Hive Membership Locked" (format "Hive membership roster is now locked and members can no longer be added or edited by Hive Creator {}." [account_id]))
            )
            (format "Locked Hive {}" [dao_id])
          )
       )
    )

    ;Update dao info
    (defun edit-dao-info
      (account_id:string dao_id:string image:string long_description:string)
        @doc " Update a DAO's info "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (MEMBERS_GUARD dao_id account_id)
              (enforce-valid-description long_description)
                (update daos-table dao_id
                    {
                        "dao_image": image,
                        "dao_long_description": long_description
                    }
                  )
                (format "Update Hive {}" [dao_id])
              )
          )
    )

    ;;///////////////////////
    ;;DAO MEMBER ACCOUNTS
    ;;//////////////////////

    ;Object used for add multiple accounts at once
    (defschema add-account-schema
    @doc " Mass adder helper schema "
      id:string
    )

    (defschema add-account-schema-create
    @doc " Mass adder helper schema "
      id:string
      can_propose:bool
    )

    ;Helper function to add multiple accounts at once when creating a DAO
    (defun mass-adder (dao_id:string new_accounts:object{add-account-schema-create})
      @doc " Adds multiple accounts to a DAO "
      (bind new_accounts {
                          "id" := new_id,
                          "can_propose" := dao_can_propose
                        }
                        (add-account dao_id new_id dao_can_propose)
      )
    )



    ;Adds account members to a dao - Permissioned
    (defun add-account (dao_id:string new_account:string can_propose:bool)
      @doc " Adds a single account to a DAO "
      (require-capability (ADD_ACCOUNT dao_id))
        (with-default-read dao-membership-ids-table new_account
          { "dao_ids" : [] }
          { "dao_ids" := t-member-ids}
          (if (= (contains new_account t-member-ids) false)
            (write dao-membership-ids-table new_account
              {
                  "dao_ids": (+ [dao_id] t-member-ids )
              }
            )
            true
          )
        )
        (with-default-read daos-table dao_id
          { "dao_updates_count" : 1, "dao_members_count" : 1, "dao_total_weight": 1.0 }
          { "dao_updates_count" := t-updates-count:integer, "dao_members_count" := t-member-count:integer, "dao_total_weight" := t-dao-total-weight}
          (insert dao-updates-table (get-2-key (+ 1 t-updates-count) dao_id)
              {
                  "message_from": "Hive",
                  "message_date": (at "block-time" (chain-data)),
                  "message_title": "New Hive Member",
                  "message": (format "New member {} has been added to the Hive" [new_account])
              }
          )
          (insert dao-accounts-table (get-user-key new_account dao_id)
            {
                "account_id": new_account,
                "account_name": new_account,
                "account_dao_id": dao_id,
                "account_banned": false,
                "account_weight": 1.0,
                "account_can_propose": can_propose,
                "account_count": t-member-count
            }
          )
          (insert dao-accounts-count-table (get-2-key t-member-count dao_id)
            {
                "account_id": new_account,
                "account_name": new_account,
                "account_dao_id": dao_id,
                "account_banned": false,
                "account_weight": 1.0,
                "account_can_propose": can_propose,
                "account_count": t-member-count
            }
          )
          (update daos-table dao_id
            {
                "dao_updates_count": (+ 1 t-updates-count),
                "dao_members_count": (+ 1 t-member-count),
                "dao_total_weight": (+ 1 t-dao-total-weight)
            }
          )
        )
    )

    ;Edit account name
    (defun edit-account-info
      (account_id:string dao_id:string new_id:string)
        @doc " Edit user account name "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (MEMBERS_GUARD dao_id account_id)
              (enforce-valid-name new_id)
              (let
                  (
                      (account-count:integer (at "account_count" (read dao-accounts-table (get-user-key account_id dao_id))))
                  )
                  (update dao-accounts-table (get-user-key account_id dao_id)
                      {
                          "account_name": new_id
                      }
                  )
                  (update dao-accounts-count-table (get-2-key account-count dao_id)
                      {
                          "account_name": new_id
                      }
                  )
                  (format "Update Account {}" [account_id])
                )
              )
          )
    )

    ;Leave a dao
    (defun leave-hive
      (account_id:string dao_id:string)
        @doc " Leave a DAO "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (MEMBERS_GUARD dao_id account_id)
              (let
                  (
                      (account-count:integer (at "account_count" (read dao-accounts-table (get-user-key account_id dao_id))))
                  )
                  (update dao-accounts-table (get-user-key account_id dao_id)
                      {
                          "account_banned": true
                      }
                  )
                  (update dao-accounts-count-table (get-2-key account-count dao_id)
                      {
                          "account_banned": true
                      }
                  )
              )
              (with-default-read dao-membership-ids-table account_id
                { "dao_ids" : [] }
                { "dao_ids" := t-member-ids}
                (let*
                        (
                          (newlist:[string]  (filter (compose (composelist) (!= dao_id)) t-member-ids) )
                        )
                        (write dao-membership-ids-table account_id
                          {
                              "dao_ids": newlist
                          }
                        )
                    )
              )
              (with-read daos-table dao_id
                { "dao_members_count" := t-member-count:integer}
                (update daos-table dao_id
                  {
                      "dao_members_count": (- t-member-count 1)
                  }
                )
              )
              (with-capability (CAN_UPDATE dao_id)
                (add-dao-update dao_id dao_id "A Member has left the Hive" (format "Member {} has left the Hive" [account_id]))
              )
              (format "Account {} has left the Hive {}" [account_id dao_id])
            )
          )
    )

    ;Removes a new member to a DAO if it isnt locked, Creator only
    (defun remove-dao-member
      (account_id:string dao_id:string member_to_remove:string)
        @doc " Adds new member to a DAO "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (CREATOR_GUARD dao_id)
                (let*
                    (
                        (dao-data (read daos-table dao_id))
                        (dao-locked:bool (at "dao_members_locked" dao-data))
                        (account-count:integer (at "account_count" (read dao-accounts-table (get-user-key account_id dao_id))))
                    )
                    (enforce (= dao-locked false) "Cannot remove members from a locked Hive" )
                    (update dao-accounts-table (get-user-key member_to_remove dao_id)
                        {
                            "account_banned": true
                        }
                    )
                    (update dao-accounts-count-table (get-2-key account-count dao_id)
                        {
                            "account_banned": true
                        }
                    )
                    (with-default-read dao-membership-ids-table member_to_remove
                      { "dao_ids" : [] }
                      { "dao_ids" := t-member-ids}
                      (let*
                              (
                                (newlist:[string]  (filter (compose (composelist) (!= dao_id)) t-member-ids) )
                              )
                              (write dao-membership-ids-table member_to_remove
                                {
                                    "dao_ids": newlist
                                }
                              )
                          )
                    )
                    (with-read daos-table dao_id
                      { "dao_members_count" := t-member-count:integer}
                      (update daos-table dao_id
                        {
                            "dao_members_count": (- t-member-count 1)
                        }
                      )
                    )
                    (with-capability (CAN_UPDATE dao_id)
                      (add-dao-update dao_id dao_id "A Member was removed from the Hive" (format "Member {} was removed from the Hive" [account_id]))
                    )
                    (format "Removed member {} from Hive {}" [member_to_remove dao_id])
              )
          )
       )
    )


    ;Adds a new member to a DAO if it isnt locked, Creator only
    (defun create-dao-member
      (account_id:string dao_id:string new_member_id:string)
        @doc " Adds new member to a DAO "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (CREATOR_GUARD dao_id)
              (let*
                (
                    (dao-data (read daos-table dao_id))
                    (dao-locked:bool (at "dao_members_locked" dao-data))
                )
                (enforce (= dao-locked false) "Cannot add members to a locked Hive" )
                ;Add new account
                (with-capability (CAN_ADD dao_id)
                  (add-account dao_id new_member_id false)
                )
                ;Return
                (format "Added new member {} to Hive {}" [new_member_id dao_id])
              )
          )
       )
    )

    ;Grants proposal creation permissions in an unlocked hive
    (defun grant-proposal-permissions
      (account_id:string dao_id:string new_member_id:string can_propose:bool)
        @doc " Grants DAO proposal permissions "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (CREATOR_GUARD dao_id)
              (let*
                (
                    (dao-data (read daos-table dao_id))
                    (dao-locked:bool (at "dao_members_locked" dao-data))
                    (account-data (read dao-accounts-table (get-user-key account_id dao_id)))
                    (account-name (at "account_name" account-data))
                    (account-count:integer (at "account_count" (read dao-accounts-table (get-user-key account_id dao_id))))
                )
                (enforce (= dao-locked false) "Cannot grant member permissions in a locked Hive" )
                (update dao-accounts-table (get-user-key new_member_id dao_id)
                  {
                      "account_can_propose": can_propose
                  }
                )
                (update dao-accounts-count-table (get-2-key account-count dao_id)
                  {
                      "account_can_propose": can_propose
                  }
                )
                (format "Member {} can now create proposals in Hive {}" [new_member_id dao_id])
              )
          )
       )
    )

    ;;/////////////////////////
    ;;DAO MESSAGES AND UPDATES
    ;;/////////////////////////

    ;Helper function to add updates to a DAO message board, Permissioned
    (defun add-dao-update
      (dao_id:string message_from:string message_title:string message:string)
        @doc " Posts an update to a Hive "
          (require-capability (ADD_UPDATE dao_id))
              (with-default-read daos-table dao_id
              { "dao_updates_count" : 1 }
              { "dao_updates_count" := t-updates-count:integer}
              ;Insert new update record
              (insert dao-updates-table (get-2-key (+ 1 t-updates-count) dao_id)
                  {
                      "message_from": "Hive",
                      "message_date": (at "block-time" (chain-data)),
                      "message_title": message_title,
                      "message": message
                  }
              )
              ;Update dao
              (update daos-table dao_id
                {
                    "dao_updates_count": (+ 1 t-updates-count)
                }
              )
              ;Emit event
              (emit-event (DAO_HIVE_UPDATED dao_id (at "block-time" (chain-data))))
            )
    )

    ;Posts a users message to a DAO's message board
    (defun create-dao-message
      (account_id:string dao_id:string message_title:string message:string)
        @doc " Posts a message to a Hive "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (MEMBERS_GUARD dao_id account_id)
                (with-default-read daos-table dao_id
                  { "dao_messages_count" : 1 }
                  { "dao_messages_count" := t-messages-count:integer}
                  (let*
                      (
                          (account-data (read dao-accounts-table (get-user-key account_id dao_id)))
                          (account-name (at "account_name" account-data))
                      )
                      ;Insert new message record
                      (insert dao-messages-table (get-2-key (+ 1 t-messages-count) dao_id)
                          {
                              "message_from": account_id,
                              "message_date": (at "block-time" (chain-data)),
                              "message_title": message_title,
                              "message": message
                          }
                      )
                      ;Update dao message count
                      (update daos-table dao_id
                        {
                            "dao_messages_count": (+ 1 t-messages-count)
                        }
                      )
                  )
                )
                ;Return
                (format "Posted new message at Hive {}" [dao_id])
              )
          )
    )

    ;;///////////////////////
    ;;DAO TREASURY POOLS
    ;;//////////////////////

    ;Creates a pool/vault within a DAO, of which the DAO governs
    (defun create-dao-treasury-pool
      (account_id:string dao_id:string token:module{fungible-v2} pool_name:string pool_description:string)
        @doc "Creates a pool for a specific token within a Hive"
        (with-capability (ACCOUNT_GUARD account_id)
         (with-capability (MEMBERS_GUARD dao_id account_id)
            (let*
                (
                    (new-treasury-account:string (create-account-key dao_id (get-token-key token)))
                    (dao-data (read daos-table dao_id))
                    (pool-count:integer (at "dao_pool_count" dao-data))
                    (account-data (read dao-accounts-table (get-user-key account_id dao_id)))
                    (account-name (at "account_name" account-data))
                )
                ;Create treasury account
                (token::create-account new-treasury-account (create-pool-guard dao_id new-treasury-account))
                ;Insert new pool record
                (insert dao-pools-table (get-2-key (+ 1 pool-count) dao_id)
                      {
                          "pool_id": (get-2-key (+ 1 pool-count) dao_id),
                          "pool_name": pool_name,
                          "pool_use_weight": false,
                          "pool_weight": 0.0,
                          "pool_description": pool_description,
                          "pool_account": new-treasury-account,
                          "pool_token": token,
                          "pool_tokenB":coin,
                          "pool_lp": false,
                          "pool_pair": "",
                          "pool_chain": (at "chain-id" (chain-data)),
                          "pool_auto": false
                      }
                )
                ;Update dao pool count
                (update daos-table dao_id
                      {
                          "dao_pool_count": (+ 1 pool-count)
                      }
                )
                ;Update dao
                (with-capability (CAN_UPDATE dao_id)
                  (add-dao-update dao_id account_id "New Hive Pool Created" (format "Hive pool {} has been created by {} to manage {} tokens" [(get-2-key (+ 1 pool-count) dao_id) account-name (get-token-key token)]))
                )
                ;Return a message
                (format "Created new pool {} for Hive {}" [new-treasury-account dao_id])
              )
          )
        )
    )

    ;Deposits tokens into a one of a DAO's pools/vaults
    (defun deposit-dao-treasury
      (account_id:string dao_id:string pool_id:string token:module{fungible-v2} amount:decimal reason:string)
        @doc " Deposits tokens to a Hive treasury pool "
        (with-capability (ACCOUNT_GUARD account_id)
            (let*
                (
                    (treasury-data (read dao-pools-table pool_id))
                    (treasury-account (at "pool_account" treasury-data))
                    (vault-name (at "pool_name" treasury-data))
                    (pool-lp (at "pool_lp" treasury-data))
                    (current-weight (at "dao_total_weight" (read daos-table dao_id)))
                    (current-user-weight (at "account_weight" (read dao-accounts-table (get-user-key account_id dao_id))))
                    (pool-weight (at "pool_weight" treasury-data))
                    (pool-u-weight (at "pool_use_weight" treasury-data))
                    (account-count:integer (at "account_count" (read dao-accounts-table (get-user-key account_id dao_id))))
                )

                ;Enforce rules
                (enforce (> amount 0.0) "Can only deposit positive amounts")
                (enforce-unit amount (token::precision))
                (enforce (= pool-lp false) "LP Token Pool")

                (if (= pool-u-weight true)
                  ;Update dao weight
                  (update daos-table dao_id
                    {
                        "dao_total_weight": (+ (* amount pool-weight) current-weight)
                    }
                  )
                  true
                )

                (if (= pool-u-weight true)
                  (let*
                      (
                        (weighted true)
                      )
                      ;Update user weight
                      (update dao-accounts-table (get-user-key account_id dao_id)
                        {
                            "account_weight": (+  (* amount pool-weight) current-user-weight)
                        }
                      )
                      (update dao-accounts-count-table (get-2-key account-count dao_id)
                        {
                            "account_weight": (+  (* amount pool-weight) current-user-weight)
                        }
                      )
                  )
                  true
                )


                (token::transfer account_id treasury-account amount)

                (with-capability (CAN_UPDATE dao_id)
                  (add-dao-update dao_id account_id (format "{} {} was deposited into Vault {}" [amount (get-token-key token) vault-name]) reason)
                )
            )

            ;Return a message
            (format "Deposited {} {} into pool {}" [amount (get-token-key token) pool_id])
        )
    )

    ;Deposits tokens into a one of a DAO's pools/vaults
    (defun pay-dao-treasury
      (account_id:string dao_id:string pool_id:string token:module{fungible-v2} amount:decimal reason:string)
        @doc " Pays tokens to a Hive treasury pool "
        (with-capability (ACCOUNT_GUARD account_id)
            (let*
                (
                    (treasury-data (read dao-pools-table pool_id))
                    (treasury-account (at "pool_account" treasury-data))
                    (vault-name (at "pool_name" treasury-data))
                    (pool-lp (at "pool_lp" treasury-data))
                    (current-weight (at "dao_total_weight" (read daos-table dao_id)))
                    (pool-weight (at "pool_weight" treasury-data))
                    (pool-u-weight (at "pool_use_weight" treasury-data))
                )

                ;Enforce rules
                (enforce (> amount 0.0) "Can only deposit positive amounts")
                (enforce-unit amount (token::precision))
                (enforce (= pool-lp false) "LP Token Pool")

                (if (= pool-u-weight true)
                  ;Update dao weight
                  (update daos-table dao_id
                    {
                        "dao_total_weight": (+ (* amount pool-weight) current-weight)
                    }
                  )
                  true
                )

                (token::transfer account_id treasury-account amount)

                (with-capability (CAN_UPDATE dao_id)
                  (add-dao-update dao_id account_id (format "{} {} was deposited into Vault {}" [amount (get-token-key token) vault-name]) reason)
                )
            )

            ;Return a message
            (format "Deposited {} {} into pool {}" [amount (get-token-key token) pool_id])
        )
    )

    ;;//////////////////////////
    ;;DAO ACTIONS + PROPOSALS
    ;;/////////////////////////

    ;All DAO proposals perform DAO actions (see below)

    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;Proposal Command Legend
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"ADD_MEMBER"
    ;action_strings = 0 = [new member account]
    ;action_decimals = []
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"REMOVE_MEMBER"
    ;action_strings = 0 = [member account to remove]
    ;action_decimals = []
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"ADJUST_DAILY_LIMIT"
    ;action_strings  = []
    ;action_decimals = []
    ;action_integers = 0 = [new limit]
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"ADJUST_THRESHOLD"
    ;action_strings  = []
    ;action_decimals = 0 = [new threshold]
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"ADJUST_VOTER_THRESHOLD"
    ;action_strings  = []
    ;action_decimals = 0 = [new threshold]
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"ADJUST_MIN_VOTETIME"
    ;action_strings  = []
    ;action_decimals = 0 = [new vote time]
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"AGAINST"
    ;action_strings  = []
    ;action_decimals = []
    ;action_integers = []
    ;Creating a proposal automatically creates an 'against' action for people to vote against the proposal- no need to create one
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"CUSTOM""
    ;action_strings = 0 = [vote option description]
    ;action_decimals = []
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"ADD_LIQUIDITY"
    ;action_strings = 0,1 = [pool id to add tokenA from, pool id to add tokenB from]
    ;action_decimals = 0,1 = [amount to add tokenA, amount to add tokenB]
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"REMOVE_LIQUIDITY"
    ;action_strings = 0 = [lp pool id that contains lp tokens]
    ;action_decimals = 0 = [amount of LP tokens to exchange]
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"SWAP"
    ;action_strings = 0,1 = [pool-id to swap tokenA from, pool-id to withdraw tokenB to]
    ;action_decimals = 0,1 = [amount of tokenA to swap, least amount of tokenB to accept]
    ;action_integers = 0 = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"WITHDRAW"
    ;action_strings = 0,1 = [pool-id to withdraw from, account to withdraw to]
    ;action_decimals = 0 = [amount to withdraw]
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"ENABLE_WEIGHT"
    ;action_strings  = []
    ;action_decimals = []
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"ENABLE_PROPOSAL_CONTROL"
    ;action_strings  = []
    ;action_decimals = []
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;"SET_WEIGHT"
    ;action_strings = 0 = [vault-id]
    ;action_decimals = 0 = [weight]
    ;action_integers = []
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

    ;Creates a new DAO proposal to perform a action (see above) at the DAO
    (defun create-dao-proposal
      (account_id:string dao_id:string run_time:decimal title:string description:string actions:[object:{pool-action-schema}] )
        @doc " Creates a proposal at a Hive "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (MEMBERS_GUARD dao_id account_id)
                (with-default-read daos-table dao_id
                  { "dao_proposal_count" : 0 }
                  { "dao_proposal_count" := t-proposal-count:integer}
                  (let*
                      (
                        (proposal_id (get-2-key (+ 1 t-proposal-count) dao_id))
                        (proposal_limit (at "dao_daily_proposal_limit" (read daos-table dao_id)))
                        (date:string (format "{}" [(at 'block-time (chain-data))]))
                        (new-action:object{pool-action-schema} {
                          "action": "AGAINST",
                          "action_strings":[],
                          "action_integers":[],
                          "action_decimals":[]
                          })
                        (new-actions:[object:{pool-action-schema}] (+  actions [new-action] ))
                        (dao-data (read daos-table dao_id))
                        (member-count (at "dao_members_count" dao-data))
                        (threshold (at "dao_threshold" dao-data))
                        (required-count (* member-count threshold))
                        (account-data (read dao-accounts-table (get-user-key account_id dao_id)))
                        (account-name (at "account_name" account-data))
                        (anyone-can-propose (at "dao_all_can_propose" dao-data))
                        (user-can-propose (at "account_can_propose" account-data))
                      )
                      (enforce (>= (length actions) 1) "Vote must contain atleast 1 option")

                      (if (= anyone-can-propose false)
                       (enforce (= user-can-propose true) "You do not have permission to make proposals in this Hive")
                       true)

                      (insert dao-proposals-table proposal_id
                          {
                              "proposal_id": proposal_id,
                              "proposal_count": (+ 1 t-proposal-count),
                              "proposal_title": (format "{}) {}" [(+ 1 t-proposal-count) title]),
                              "proposal_description": description,
                              "proposal_start_time": (at "block-time" (chain-data)),
                              "proposal_end_time": (add-time (at "block-time" (chain-data)) run_time),
                              "proposal_completed_time": (at "block-time" (chain-data)),
                              "proposal_completed_action": 'null,
                              "proposal_completed_consensus": required-count,
                              "proposal_completed_voter_consensus": required-count,
                              "proposal_dao_id": dao_id,
                              "proposal_options_count": 1,
                              "proposal_creator": account_id,
                              "proposal_completed": false,
                              "proposal_chain": (at "chain-id" (chain-data))
                          }
                      )
                      (update daos-table dao_id
                        {
                            "dao_proposal_count": (+ 1 t-proposal-count)
                        }
                      )

                      (with-default-read user-proposition-records (get-user-key account_id (take 11 date))
                        { "pr_proposition_count" : 0, "pr_propositions" : [] }
                        { "pr_proposition_count" := t-user-proposal-count:integer, "pr_propositions" := t-user-propositions}

                        (enforce (<= t-user-proposal-count proposal_limit) "Daily proposal limit reached")

                        (write user-proposition-records (get-user-key account_id (take 11 date))
                            {
                                "pr_account": proposal_id,
                                "pr_date": (take 11 date),
                                "pr_proposition_count": (+ 1 t-user-proposal-count),
                                "pr_propositions": (+  t-user-propositions [proposal_id] )
                            }
                        )
                      )

                      (with-capability (CAN_ADD_OPTION proposal_id)
                        (map (_add-option proposal_id) new-actions)
                      )

                      (with-capability (CAN_UPDATE dao_id)
                        (add-dao-update dao_id dao_id (format "Proposal #{} has been created!" [(+ 1 t-proposal-count)]) (format "New Proposal #{}) {} has been created at the Hive on {} by {}" [(+ 1 t-proposal-count) title (at "block-time" (chain-data)) account-name]))
                      )

                      (format "Created proposal {} at Hive {}" [(get-2-key (+ 1 t-proposal-count) dao_id) dao_id])
                  )
                )
              )
          )
    )


    ;Helper function to add voting options to a proposal
    (defun _add-option (proposal_id:string action:object:{pool-action-schema})
      @doc " Adds a voting option to a proposal - Permissioned "
      (require-capability (ADD_OPTION proposal_id))
      (let*
          (
            (proposal-data (read dao-proposals-table proposal_id ["proposal_options_count"]))
            (options-count (at "proposal_options_count" proposal-data))
          )
          ;Enforce rules depending on action
            (bind action { "action" := t_action:string, "action_strings" := t_action_strings:[string], "action_integers" := t_action_integers:[integer], "action_decimals" := t_action_decimals:[decimal] }
              (cond ((= t_action "WITHDRAW") (let*
                                                (
                                                  (WITHDRAW true)
                                                  (pool-id (at 0 t_action_strings))
                                                  (pool-data (read dao-pools-table pool-id) ["pool_token", "pool_account", "pool_lp"])
                                                  (pool-token:module{fungible-v2} (at "pool_token" pool-data))
                                                  (pool-account (at "pool_account" pool-data))
                                                  (withdraw-to (at 1 t_action_strings))
                                                  (withdraw-amount (at 0 t_action_decimals))
                                                  (has-account (try "default" (pool-token::details withdraw-to)))
                                                  (pool-balance (pool-token::get-balance pool-account))
                                                  (pool-lp (at "pool_lp" pool-data))
                                                )
                                                (enforce (= pool-lp false) "LP Pool")
                                                (enforce (>= withdraw-amount 0.0) "Positive withdraw amounts only")
                                                (enforce (!= (typeof has-account) "string") "The account you are withdrawing to doesnt exist")
                                                (enforce-unit withdraw-amount (pool-token::precision))
                                                (enforce (>= pool-balance withdraw-amount) "Insufficient funds")
                                                (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Withdraw {} {} to {}" [withdraw-amount pool-token withdraw-to]) action )

                                              ))
                    ((= t_action "SWAP") (let*
                                                (
                                                  (SWAP true)
                                                  (from-pool-id (at 0 t_action_strings))
                                                  (to-pool-id (at 1 t_action_strings))
                                                  (swap-in-amount (at 0 t_action_decimals))
                                                  (swap-out-amount (at 1 t_action_decimals))
                                                  (from-pool-data (read dao-pools-table from-pool-id) ["pool_token", "pool_account"])
                                                  (to-pool-data (read dao-pools-table to-pool-id) ["pool_token", "pool_account"])
                                                  (from-pool-tokenA:module{fungible-v2} (at "pool_token" from-pool-data))
                                                  (to-pool-tokenB:module{fungible-v2} (at "pool_token" to-pool-data))
                                                  (from-pool-account (at "pool_account" from-pool-data))
                                                  (to-pool-account (at "pool_account" to-pool-data))
                                                  (from-pool-balance (from-pool-tokenA::get-balance from-pool-account))
                                                  (pool-lp-A (at "pool_lp" from-pool-data))
                                                  (pool-lp-B (at "pool_lp" to-pool-data))
                                                )
                                                (enforce (= pool-lp-A false) "Cannot withdraw LP Pool")
                                                (enforce (= pool-lp-B false) "Cannot deposit LP Pool")
                                                (enforce (> swap-in-amount 0.0) "Positive swap-in amounts only")
                                                (enforce (>= swap-out-amount 0.0) "Positive withdraw amounts only")
                                                (enforce-unit swap-in-amount (from-pool-tokenA::precision))
                                                (enforce-unit swap-out-amount (to-pool-tokenB::precision))
                                                (enforce (>= from-pool-balance swap-in-amount) "Insufficient funds")
                                                (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Swap {} {} to {}" [swap-in-amount from-pool-tokenA to-pool-tokenB]) action )
                                              ))
                      ((= t_action "ADD_LIQUIDITY") (let*
                                                (
                                                  (ADD_LIQUIDITY true)
                                                  (pool-id-A (at 0 t_action_strings))
                                                  (pool-id-B (at 1 t_action_strings))
                                                  (add-amount-A (at 0 t_action_decimals))
                                                  (add-amount-B (at 1 t_action_decimals))
                                                  (pool-A-data (read dao-pools-table pool-id-A) ["pool_token", "pool_account"])
                                                  (pool-B-data (read dao-pools-table pool-id-B) ["pool_token", "pool_account"])
                                                  (tokenA:module{fungible-v2} (at "pool_token" pool-A-data))
                                                  (tokenB:module{fungible-v2} (at "pool_token" pool-B-data))
                                                  (pool-A-account (at "pool_account" pool-A-data))
                                                  (pool-B-account (at "pool_account" pool-B-data))
                                                  (pool-A-balance (tokenA::get-balance pool-A-account))
                                                  (pool-B-balance (tokenB::get-balance pool-B-account))
                                                )
                                                (enforce (> add-amount-A 0.0) "Positive swap-in amounts only")
                                                (enforce (> add-amount-B 0.0) "Positive withdraw amounts only")
                                                (enforce-unit add-amount-A (tokenA::precision))
                                                (enforce-unit add-amount-B (tokenB::precision))
                                                (enforce (>= pool-A-balance add-amount-A) "Insufficient funds")
                                                (enforce (>= pool-B-balance add-amount-B) "Insufficient funds")
                                                (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Add {} {} + {} {} Liquidity to KDS" [add-amount-A tokenA add-amount-B tokenB]) action )
                                              ))
                    ((= t_action "REMOVE_LIQUIDITY") (let*
                                                (
                                                  (REMOVE_LIQUIDITY true)
                                                  (lp-pool-id (at 0 t_action_strings))
                                                  (remove-amount (at 0 t_action_decimals))
                                                  (lp-pool-data (read dao-pools-table lp-pool-id))
                                                  (tokenA:module{fungible-v2} (at "pool_token" lp-pool-data))
                                                  (tokenB:module{fungible-v2} (at "pool_tokenB" lp-pool-data))
                                                  (lp-pool-account (at "pool_account" lp-pool-data))
                                                  (lp-pool-pair (at "pool_pair" lp-pool-data))
                                                  (lp-pool-balance (test.dao-hive-reference.kds-tokens-get-balance lp-pool-pair lp-pool-account))
                                                )
                                                (enforce (> remove-amount 0.0) "Positive remove lp amounts only")
                                                (enforce (>= lp-pool-balance remove-amount) "Insufficient funds")
                                                (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Remove {} {} Liquidity from KDS" [remove-amount lp-pool-pair]) action )
                                              ))
                    ((= t_action "ADJUST_DAILY_LIMIT") (let*
                                                        (
                                                          (ADJUST_THRESHOLD true)
                                                          (new-limit (at 0 t_action_integers))
                                                        )
                                                        (enforce (> new-limit 0) "Positive daily limits only")
                                                        (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Adjust all Hive member daily proposal limits to {}" [new-limit]) action )
                                                      ))
                    ((= t_action "ADJUST_THRESHOLD") (let*
                                                        (
                                                          (ADJUST_THRESHOLD true)
                                                          (new-threshold (at 0 t_action_decimals))
                                                        )
                                                        (enforce (> new-threshold 0.0) "Positive thresholds only")
                                                        (enforce (<= new-threshold 1.0) "Thresholds must be <= 1.0")
                                                        (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Adjust the Hive's voting consensus threshold to {}" [new-threshold]) action )
                                                      ))
                    ((= t_action "ADJUST_VOTER_THRESHOLD") (let*
                                                        (
                                                          (ADJUST_VOTER_THRESHOLD true)
                                                          (new-threshold (at 0 t_action_decimals))
                                                        )
                                                        (enforce (> new-threshold 0.0) "Positive thresholds only")
                                                        (enforce (<= new-threshold 1.0) "Thresholds must be <= 1.0")
                                                        (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Adjust the Hive's required voters consensus threshold to {}" [new-threshold]) action )
                                                      ))
                    ((= t_action "ADJUST_MIN_VOTETIME") (let*
                                                        (
                                                          (ADJUST_VOTETIME true)
                                                          (new-time (at 0 t_action_decimals))
                                                        )
                                                        (enforce (> new-time 0.0) "Positive vote times only")
                                                        (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Adjust the Hive's minimum voting time to {} seconds" [new-time]) action )
                                                      ))
                    ((= t_action "ADD_MEMBER") (let*
                                                (
                                                  (ADD_MEMBER true)
                                                  (new-id (at 0 t_action_strings))
                                                  (has-account (try "default" (coin.details new-id)))
                                                )
                                                (enforce (!= (typeof has-account) "string") "New member requires coin account")
                                                (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Add new member {} to the Hive" [new-id]) action )
                                              ))
                    ((= t_action "REMOVE_MEMBER") (let*
                                                (
                                                  (REMOVE_MEMBER true)
                                                  (member-id (at 0 t_action_strings))
                                                )
                                                (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Remove member {} from the Hive" [member-id]) action )
                                              ))
                    ((= t_action "AGAINST") (let*
                                                  (
                                                    (AGAINST true)
                                                  )
                                                  (insert-option proposal_id options-count (get-2-key options-count proposal_id) "Against" action )
                                                ))
                    ((= t_action "ENABLE_WEIGHT") (let*
                                                        (
                                                          (ENABLE_WEIGHT true)
                                                        )
                                                        (insert-option proposal_id options-count (get-2-key options-count proposal_id) "Switch the Hive's consensus method to Weight Mode" action )
                                                      ))
                    ((= t_action "ENABLE_PROPOSAL_CONTROL") (let*
                                                        (
                                                          (ENABLE_PROPOSAL_CONTROL true)
                                                        )
                                                        (insert-option proposal_id options-count (get-2-key options-count proposal_id) "Switch the Hive to only allow proposals to be created by designated members" action )
                                                      ))
                    ((= t_action "SET_WEIGHT") (let*
                                                        (
                                                          (SET_WEIGHT true)
                                                          (new-weight (at 0 t_action_decimals))
                                                          (pool-id (at 0 t_action_strings))
                                                          (lp-pool-data (read dao-pools-table pool-id))
                                                          (pool-name (at "pool_name" lp-pool-data))
                                                        )
                                                        (enforce (> new-weight 0.0) "Positive Weights Only")
                                                        (insert-option proposal_id options-count (get-2-key options-count proposal_id) (format "Adjust the Deposit Weight of Vault {} to {}" [pool-name new-weight]) action )
                                                      ))
                     ((constantly true) (let*
                                                  (
                                                    (CUSTOM true)
                                                    (new-description (at 0 t_action_strings))
                                                  )
                                                  (insert-option proposal_id options-count (get-2-key options-count proposal_id) new-description action )
                                                ))
                     false)
           )

          )
    )

    ;Helper function to insert voting options
    (defun insert-option
      (proposal_id:string current_option_count:integer key:string description:string action:object:{pool-action-schema})
      @doc " Adds a voting option to a proposal - Permissioned "
      (require-capability (ADD_OPTION proposal_id))
        ;Insert new vote option
        (insert dao-votes-table key
            {
                "vote_count": 0,
                "vote_option": action,
                "vote_description": description,
                "vote_weight": 0.0
            }
        )
        ;Update dao proposal count
        (update dao-proposals-table proposal_id
          {
              "proposal_options_count": (+ 1 current_option_count)
          }
        )
    )

    ;Votes for or against a DAO proposal
    (defun create-proposal-vote
      (account_id:string dao_id:string proposal_id:string vote:integer)
        @doc " Votes on a proposal at a Hive "
          (with-capability (ACCOUNT_GUARD account_id)
            (with-capability (MEMBERS_GUARD dao_id account_id)
              (let*
                  (
                    (dao-data (read daos-table dao_id))
                    (proposal-data (read dao-proposals-table proposal_id))
                    (vote-id (get-2-key vote proposal_id))
                    (vote-data (read dao-votes-table vote-id))
                    (vote-count (at "vote_count" vote-data) )
                    (end-time (at "proposal_end_time" proposal-data))
                    (ended (at "proposal_completed" proposal-data))
                    (options-count (at "proposal_options_count" proposal-data))
                    (member-count (at "dao_members_count" dao-data))
                    (threshold (at "dao_threshold" dao-data))
                    (required-count (* member-count threshold))
                    (total-weight (at "dao_total_weight" dao-data))
                    (vote-weight (at "vote_weight" vote-data) )
                    (user-weight (at "account_weight" (read dao-accounts-table (get-user-key account_id dao_id))))
                    (do-weight (at "dao_use_weight" dao-data))
                    (required-weight (* total-weight threshold))
                    (weighted-voter-threshold (at "dao_voter_threshold" dao-data))
                    (weighted-required-voters (* member-count weighted-voter-threshold))
                  )
                  ;Enforce rules
                  (enforce (> (diff-time end-time (at "block-time" (chain-data))) 0.0 ) "This proposal has already ended")
                  (enforce (= ended false) "This proposal has passed already")
                  (enforce (>= vote 0) "This voting option doesnt exist")
                  (enforce (<= vote options-count) "This voting option doesnt exist")

                  ;Update vote count/weight
                  (update dao-votes-table vote-id
                    {
                        "vote_count": (+ 1 vote-count),
                        "vote_weight": (+ user-weight vote-weight)
                    }
                  )

                  ;Record vote
                  (insert user-vote-records (get-user-key account_id proposal_id)
                      {
                          "vote_account": account_id,
                          "vote_proposal": proposal_id,
                          "vote_time": (at 'block-time (chain-data)),
                          "vote_option": vote
                      }
                  )

                  ;Check for consensus
                  (if (= do-weight true)
                  ;Weighted Consensus Check
                    (if (and (>= (+ user-weight vote-weight) required-weight) (>= (* (+ 1 vote-count) 1.0) weighted-required-voters))
                      (let
                          (
                            (VOTE-PASSED true)
                          )
                          (with-capability (CAN_COMPLETE proposal_id dao_id)
                            (complete-proposal-action dao_id proposal_id vote-id)
                          )
                          (update dao-proposals-table proposal_id
                            {
                                "proposal_completed_consensus": (+ user-weight vote-weight),
                                "proposal_completed_voter_consensus": (* (+ 1 vote-count) 1.0)
                            }
                          )
                      )
                      true
                    )
                  ;Non-Weighted Consensus Check
                    (if (>=  (* (+ 1 vote-count) 1.0) required-count)
                        (let
                            (
                              (VOTE-PASSED true)
                            )
                            (with-capability (CAN_COMPLETE proposal_id dao_id)
                              (complete-proposal-action dao_id proposal_id vote-id)
                            )
                            (update dao-proposals-table proposal_id
                              {
                                  "proposal_completed_consensus": required-weight,
                                  "proposal_completed_voter_consensus": required-count
                              }
                            )
                        )
                        true
                    )
                  )
                  ;Return
                  (format "Voted for option {} on proposal {}" [vote proposal_id])
              )
            )
          )
    )


    ;Processes proposal actions through a decision pipeline when a proposal is completed
    (defun complete-proposal-action (dao_id:string proposal_id:string vote_id:string)
      (require-capability (COMPLETE_ACTION))
        (let*
              (
                (vote-data (read dao-votes-table vote_id))
                (action-data:object{pool-action-schema} (at "vote_option" vote-data))
                (proposal_count (at "proposal_count" (read dao-proposals-table proposal_id)))
              )
              ;Handle new voted action
              (bind action-data { "action" := t_action:string, "action_strings" := t_action_strings:[string], "action_integers" := t_action_integers:[integer], "action_decimals" := t_action_decimals:[decimal] }
                (cond ((= t_action "WITHDRAW") (let*
                                                    (
                                                      (WITHDRAW true)
                                                      (pool-id (at 0 t_action_strings))
                                                      (pool-data (read dao-pools-table pool-id))
                                                      (pool-token:module{fungible-v2} (at "pool_token" pool-data))
                                                      (pool-account (at "pool_account" pool-data))
                                                      (withdraw-to (at 1 t_action_strings))
                                                      (withdraw-amount (at 0 t_action_decimals))
                                                    )

                                                    (install-capability (pool-token::TRANSFER pool-account withdraw-to withdraw-amount))
                                                    (with-capability (PRIVATE_RESERVE dao_id pool-account) (pool-token::transfer pool-account withdraw-to withdraw-amount))

                                                    (with-capability (CAN_UPDATE dao_id)
                                                      (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "{} {} was successfully withdrawn to account {}" [withdraw-amount (get-token-key pool-token) withdraw-to]))
                                                    )

                                                    (update dao-proposals-table proposal_id
                                                      {
                                                          "proposal_completed": true,
                                                          "proposal_completed_time": (at 'block-time (chain-data)),
                                                          "proposal_completed_action": t_action
                                                      }
                                                    )


                                                  ))
                        ((= t_action "SWAP") (let*
                                                    (
                                                      (SWAP true)
                                                      (from-pool-id (at 0 t_action_strings))
                                                      (to-pool-id (at 1 t_action_strings))
                                                      (swap-in-amount (at 0 t_action_decimals))
                                                      (swap-out-amount (at 1 t_action_decimals))
                                                      (from-pool-data (read dao-pools-table from-pool-id) ["pool_token", "pool_account"])
                                                      (to-pool-data (read dao-pools-table to-pool-id) ["pool_token", "pool_account"])
                                                      (from-pool-tokenA:module{fungible-v2} (at "pool_token" from-pool-data))
                                                      (to-pool-tokenB:module{fungible-v2} (at "pool_token" to-pool-data))
                                                      (from-pool-account (at "pool_account" from-pool-data))
                                                      (to-pool-account (at "pool_account" to-pool-data))
                                                      (from-pool-balance (from-pool-tokenA::get-balance from-pool-account))
                                                      (swap-account (at 'account (test.dao-hive-reference.kds-get-pair from-pool-tokenA to-pool-tokenB)))
                                                    )
                                                    ;(swap-account (at 'account (swap.exchange.get-pair from-pool-tokenA to-pool-tokenB)))
                                                    (install-capability (from-pool-tokenA::TRANSFER from-pool-account swap-account swap-in-amount))
                                                    (with-capability (PRIVATE_RESERVE dao_id from-pool-account)
                                                      ;(swap.exchange.swap-exact-in swap-in-amount 0.0 [from-pool-tokenA to-pool-tokenB] from-pool-account to-pool-account (at "guard" (to-pool-tokenB::details to-pool-account)) )
                                                      (test.dao-hive-reference.kds-swap-exact-in swap-in-amount from-pool-tokenA to-pool-tokenB from-pool-account to-pool-account)
                                                    )
                                                    (with-capability (CAN_UPDATE dao_id)
                                                      (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "Swapped {} {} to {}" [swap-in-amount (get-token-key from-pool-tokenA) (get-token-key to-pool-tokenB)]))
                                                    )
                                                    (update dao-proposals-table proposal_id
                                                      {
                                                          "proposal_completed": true,
                                                          "proposal_completed_time": (at 'block-time (chain-data)),
                                                          "proposal_completed_action": t_action
                                                      }
                                                    )
                                                  ))
                      ((= t_action "ADD_LIQUIDITY") (let*
                                                (
                                                  (ADD_LIQUIDITY true)
                                                  (pool-id-A (at 0 t_action_strings))
                                                  (pool-id-B (at 1 t_action_strings))
                                                  (add-amount-A (at 0 t_action_decimals))
                                                  (add-amount-B (at 1 t_action_decimals))
                                                  (pool-A-data (read dao-pools-table pool-id-A) ["pool_token", "pool_account", "pool_use_weight", "pool_weight"])
                                                  (pool-B-data (read dao-pools-table pool-id-B) ["pool_token", "pool_account", "pool_use_weight", "pool_weight"])
                                                  (pool-A-weight (at "pool_weight" pool-A-data))
                                                  (pool-B-weight (at "pool_weight" pool-B-data))
                                                  (pool-A-use-weight (at "pool_use_weight" pool-A-data))
                                                  (pool-B-use-weight (at "pool_use_weight" pool-B-data))
                                                  (tokenA:module{fungible-v2} (at "pool_token" pool-A-data))
                                                  (tokenB:module{fungible-v2} (at "pool_token" pool-B-data))
                                                  (pool-A-account (at "pool_account" pool-A-data))
                                                  (pool-B-account (at "pool_account" pool-B-data))
                                                  (pool-A-balance (tokenA::get-balance pool-A-account))
                                                  (pool-B-balance (tokenB::get-balance pool-B-account))
                                                  (swap-account (at 'account (test.dao-hive-reference.kds-get-pair tokenA tokenB)))
                                                  (new-treasury-account:string (create-account-key dao_id proposal_id))
                                                  (dao-data (read daos-table dao_id))
                                                  (pool-count:integer (at "dao_pool_count" dao-data))
                                                  (lp-pool-pair (test.dao-hive-reference.kds-get-pair-key tokenA tokenB))
                                                )
                                                ;(swap-account (at 'account (swap.exchange.get-pair tokenA tokenB)))
                                                ;(lp-pool-pair (swap.exchange.get-pair-key tokenA tokenB))
                                                (tokenA::create-account new-treasury-account (create-pool-guard dao_id new-treasury-account))
                                                (tokenB::create-account new-treasury-account (create-pool-guard dao_id new-treasury-account))
                                                (insert dao-pools-table (get-2-key (+ 1 pool-count) dao_id)
                                                      {
                                                          "pool_id": (get-2-key (+ 1 pool-count) dao_id),
                                                          "pool_name": (format "{} Pool {}" [tokenA (+ 1 pool-count)]),
                                                          "pool_use_weight": false,
                                                          "pool_weight": 0.0,
                                                          "pool_description": "Pool auto-created to manage and add liquidity at KDS",
                                                          "pool_account": new-treasury-account,
                                                          "pool_token": tokenA,
                                                          "pool_tokenB": coin,
                                                          "pool_lp": false,
                                                          "pool_pair": "",
                                                          "pool_chain": (at "chain-id" (chain-data)),
                                                          "pool_auto": true
                                                      }
                                                )
                                                (insert dao-pools-table (get-2-key (+ 2 pool-count) dao_id)
                                                      {
                                                          "pool_id": (get-2-key (+ 2 pool-count) dao_id),
                                                          "pool_name": (format "{} Pool {}" [tokenB (+ 2 pool-count)]),
                                                          "pool_use_weight": false,
                                                          "pool_weight": 0.0,
                                                          "pool_description": "Pool auto-created to manage and add liquidity at KDS",
                                                          "pool_account": new-treasury-account,
                                                          "pool_token": tokenB,
                                                          "pool_tokenB": coin,
                                                          "pool_lp": false,
                                                          "pool_pair": "",
                                                          "pool_chain": (at "chain-id" (chain-data)),
                                                          "pool_auto": true
                                                      }
                                                )
                                                (insert dao-pools-table (get-2-key (+ 3 pool-count) dao_id)
                                                      {
                                                          "pool_id": (get-2-key (+ 3 pool-count) dao_id),
                                                          "pool_name": (format "{} Pool {}" [lp-pool-pair (+ 3 pool-count)]),
                                                          "pool_use_weight": false,
                                                          "pool_weight": 0.0,
                                                          "pool_description": "Pool auto-created to manage and add liquidity at KDS",
                                                          "pool_account": new-treasury-account,
                                                          "pool_token": tokenA,
                                                          "pool_tokenB": tokenB,
                                                          "pool_lp": true,
                                                          "pool_pair": lp-pool-pair,
                                                          "pool_chain": (at "chain-id" (chain-data)),
                                                          "pool_auto": true
                                                      }
                                                )
                                                (update daos-table dao_id
                                                      {
                                                          "dao_pool_count": (+ 3 pool-count)
                                                      }
                                                )
                                                (install-capability (tokenA::TRANSFER pool-A-account new-treasury-account add-amount-A))
                                                (with-capability (PRIVATE_RESERVE dao_id pool-A-account) (tokenA::transfer pool-A-account new-treasury-account add-amount-A))
                                                (install-capability (tokenB::TRANSFER pool-B-account new-treasury-account add-amount-B))
                                                (with-capability (PRIVATE_RESERVE dao_id pool-B-account) (tokenB::transfer pool-B-account new-treasury-account add-amount-B))
                                                (install-capability (tokenA::TRANSFER new-treasury-account swap-account add-amount-A))
                                                (install-capability (tokenB::TRANSFER new-treasury-account swap-account add-amount-B))
                                                (with-capability (PRIVATE_RESERVE dao_id new-treasury-account)
                                                  (test.dao-hive-reference.kds-add-liquidity tokenA tokenB add-amount-A add-amount-B new-treasury-account (create-pool-guard dao_id new-treasury-account))
                                                )
                                                ;(swap.exchange.add-liquidity tokenA tokenB add-amount-A add-amount-B 0.0 0.0 new-treasury-account new-treasury-account (create-pool-guard dao_id new-treasury-account))
                                                (with-capability (CAN_UPDATE dao_id)
                                                  (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "The Hive added {} {} and {} {} token liquidity to the KDS - 2 Pools were added to the Hive in order to perform this action and may contain some left over tokens from KDS." [add-amount-A tokenA add-amount-B tokenB]))
                                                )
                                                (update dao-proposals-table proposal_id
                                                  {
                                                      "proposal_completed": true,
                                                      "proposal_completed_time": (at 'block-time (chain-data)),
                                                      "proposal_completed_action": t_action
                                                  }
                                                )
                                              ))
                      ((= t_action "REMOVE_LIQUIDITY") (let*
                                                (
                                                  (REMOVE_LIQUIDITY true)
                                                  (lp-pool-id (at 0 t_action_strings))
                                                  (remove-amount (at 0 t_action_decimals))
                                                  (lp-pool-data (read dao-pools-table lp-pool-id))
                                                  (tokenA:module{fungible-v2} (at "pool_token" lp-pool-data))
                                                  (tokenB:module{fungible-v2} (at "pool_tokenB" lp-pool-data))
                                                  (lp-pool-account (at "pool_account" lp-pool-data))
                                                  (lp-pool-pair (at "pool_pair" lp-pool-data))
                                                  (lp-pool-balance (test.dao-hive-reference.kds-tokens-get-balance lp-pool-pair lp-pool-account))
                                                  (new-treasury-account:string (create-account-key dao_id proposal_id))
                                                  (dao-data (read daos-table dao_id))
                                                  (pool-count:integer (at "dao_pool_count" dao-data))
                                                  (swap-account (at 'account (test.dao-hive-reference.kds-get-pair tokenA tokenB)))
                                                )
                                                (tokenA::create-account new-treasury-account (create-pool-guard dao_id new-treasury-account))
                                                (tokenB::create-account new-treasury-account (create-pool-guard dao_id new-treasury-account))
                                                (insert dao-pools-table (get-2-key (+ 1 pool-count) dao_id)
                                                      {
                                                          "pool_id": (get-2-key (+ 1 pool-count) dao_id),
                                                          "pool_name": (format "{} Pool {}" [tokenA (+ 1 pool-count)]),
                                                          "pool_use_weight": false,
                                                          "pool_weight": 0.0,
                                                          "pool_description": "Pool auto-created to withdraw liquidity from KDS",
                                                          "pool_account": new-treasury-account,
                                                          "pool_token": tokenA,
                                                          "pool_tokenB": coin,
                                                          "pool_lp": false,
                                                          "pool_pair": "",
                                                          "pool_chain": (at "chain-id" (chain-data)),
                                                          "pool_auto": true
                                                      }
                                                )
                                                (insert dao-pools-table (get-2-key (+ 2 pool-count) dao_id)
                                                      {
                                                          "pool_id": (get-2-key (+ 2 pool-count) dao_id),
                                                          "pool_name": (format "{} Pool {}" [tokenB (+ 2 pool-count)]),
                                                          "pool_use_weight": false,
                                                          "pool_weight": 0.0,
                                                          "pool_description": "Pool auto-created to withdraw liquidity from KDS",
                                                          "pool_account": new-treasury-account,
                                                          "pool_token": tokenB,
                                                          "pool_tokenB": coin,
                                                          "pool_lp": false,
                                                          "pool_pair": "",
                                                          "pool_chain": (at "chain-id" (chain-data)),
                                                          "pool_auto": true
                                                      }
                                                )
                                                (update daos-table dao_id
                                                      {
                                                          "dao_pool_count": (+ 2 pool-count)
                                                      }
                                                )
                                                (with-capability (PRIVATE_RESERVE dao_id lp-pool-account)
                                                  (test.dao-hive-reference.kds-remove-liquidity tokenA tokenB remove-amount lp-pool-account new-treasury-account (create-pool-guard dao_id new-treasury-account) lp-pool-pair swap-account remove-amount)
                                                )
                                                ; (install-capability (swap.tokens.TRANSFER lp-pool-pair lp-pool-account swap-account remove-amount))
                                                ; (with-capability (PRIVATE_RESERVE dao_id lp-pool-account)
                                                ;   (test.dao-hive-reference.kds-remove-liquidity tokenA tokenB remove-amount lp-pool-account new-treasury-account (create-pool-guard dao_id new-treasury-account))
                                                ; )
                                                ;(swap.exchange.remove-liquidity tokenA tokenB remove-amount 0.0 0.0 lp-pool-account new-treasury-account (create-pool-guard dao_id new-treasury-account))
                                                (with-capability (CAN_UPDATE dao_id)
                                                  (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "The Hive removed {} {} liquidity from KDS - 2 Pools were added to the Hive and contain all the tokens withdrawn while performing this action." [remove-amount lp-pool-pair]))
                                                )
                                                (update dao-proposals-table proposal_id
                                                  {
                                                      "proposal_completed": true,
                                                      "proposal_completed_time": (at 'block-time (chain-data)),
                                                      "proposal_completed_action": t_action
                                                  }
                                                )
                                              ))
                      ((= t_action "ADJUST_DAILY_LIMIT") (let*
                                                            (
                                                              (ADJUST_DAILY_LIMIT true)
                                                              (new-limit (at 0 t_action_integers))
                                                            )
                                                            (update daos-table dao_id
                                                              {
                                                                  "dao_daily_proposal_limit": new-limit
                                                              }
                                                            )
                                                            (with-capability (CAN_UPDATE dao_id)
                                                              (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "The Hive's daily proposal limit for all members has been adjusted to {}" [new-limit]))
                                                            )
                                                            (update dao-proposals-table proposal_id
                                                              {
                                                                  "proposal_completed": true,
                                                                  "proposal_completed_time": (at 'block-time (chain-data)),
                                                                  "proposal_completed_action": t_action
                                                              }
                                                            )
                                                          ))
                      ((= t_action "ADJUST_THRESHOLD") (let*
                                                            (
                                                              (ADJUST_THRESHOLD true)
                                                              (new-threshold (at 0 t_action_decimals))
                                                            )
                                                            (update daos-table dao_id
                                                              {
                                                                  "dao_threshold": new-threshold
                                                              }
                                                            )
                                                            (with-capability (CAN_UPDATE dao_id)
                                                              (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "The Hive's voting threshold has been adjusted to {}" [new-threshold]))
                                                            )
                                                            (update dao-proposals-table proposal_id
                                                              {
                                                                  "proposal_completed": true,
                                                                  "proposal_completed_time": (at 'block-time (chain-data)),
                                                                  "proposal_completed_action": t_action
                                                              }
                                                            )
                                                          ))
                      ((= t_action "ADJUST_VOTER_THRESHOLD") (let*
                                                            (
                                                              (ADJUST_VOTER_THRESHOLD true)
                                                              (new-threshold (at 0 t_action_decimals))
                                                            )
                                                            (update daos-table dao_id
                                                              {
                                                                  "dao_voter_threshold": new-threshold
                                                              }
                                                            )
                                                            (with-capability (CAN_UPDATE dao_id)
                                                              (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "The Hive's required voters threshold has been adjusted to {}" [new-threshold]))
                                                            )
                                                            (update dao-proposals-table proposal_id
                                                              {
                                                                  "proposal_completed": true,
                                                                  "proposal_completed_time": (at 'block-time (chain-data)),
                                                                  "proposal_completed_action": t_action
                                                              }
                                                            )
                                                          ))
                      ((= t_action "ADJUST_MIN_VOTETIME") (let*
                                                            (
                                                              (ADJUST_VOTETIME true)
                                                              (new-time (at 0 t_action_decimals))
                                                            )
                                                            (update daos-table dao_id
                                                              {
                                                                  "dao_minimum_proposal_time": new-time
                                                              }
                                                            )
                                                            (with-capability (CAN_UPDATE dao_id)
                                                              (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "The Hive's minimum required voting time has been adjusted to {} seconds" [new-time]))
                                                            )
                                                            (update dao-proposals-table proposal_id
                                                              {
                                                                  "proposal_completed": true,
                                                                  "proposal_completed_time": (at 'block-time (chain-data)),
                                                                  "proposal_completed_action": t_action
                                                              }
                                                            )
                                                          ))
                      ((= t_action "ADD_MEMBER") (let*
                                                      (
                                                        (ADD_MEMBER true)
                                                        (new-member-id (at 0 t_action_strings))
                                                      )
                                                      (with-capability (CAN_ADD dao_id)
                                                        (add-account dao_id new-member-id false)
                                                      )
                                                      (with-capability (CAN_UPDATE dao_id)
                                                        (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "New member {} has been voted into the Hive" [new-member-id]))
                                                      )
                                                      (update dao-proposals-table proposal_id
                                                        {
                                                            "proposal_completed": true,
                                                            "proposal_completed_time": (at 'block-time (chain-data)),
                                                            "proposal_completed_action": t_action
                                                        }
                                                      )
                                                    ))
                      ((= t_action "REMOVE_MEMBER") (let*
                                                      (
                                                        (REMOVE_MEMBER true)
                                                        (remove-member-id (at 0 t_action_strings))
                                                        (dao-data (read daos-table dao_id))
                                                        (dao-creator (at "dao_creator" dao-data))
                                                        (dao-locked (at "dao_members_locked" dao-data))
                                                        (account-count:integer (at "account_count" (read dao-accounts-table (get-user-key remove-member-id dao_id))))
                                                      )
                                                      (if (and (= remove-member-id dao-creator) (= dao-locked true) )
                                                        (with-capability (CAN_UPDATE dao_id)
                                                          (add-dao-update dao_id dao_id (format "Proposal {} Failed" [proposal_count]) (format "Can not remove Hive Creator {} from the Hive" [remove-member-id]))
                                                        )
                                                        (let*
                                                            (
                                                              (REMOVE true)
                                                            )
                                                            (update dao-accounts-table (get-user-key remove-member-id dao_id)
                                                                {
                                                                    "account_banned": true
                                                                }
                                                            )
                                                            (update dao-accounts-count-table (get-2-key account-count dao_id)
                                                                {
                                                                    "account_banned": true
                                                                }
                                                            )
                                                            (with-default-read dao-membership-ids-table remove-member-id
                                                              { "dao_ids" : [] }
                                                              { "dao_ids" := t-member-ids}
                                                              (let*
                                                                      (
                                                                        (newlist:[string]  (filter (compose (composelist) (!= dao_id)) t-member-ids) )
                                                                      )
                                                                      (write dao-membership-ids-table remove-member-id
                                                                        {
                                                                            "dao_ids": newlist
                                                                        }
                                                                      )
                                                                  )
                                                            )
                                                            (with-read daos-table dao_id
                                                              { "dao_members_count" := t-member-count:integer}
                                                              (update daos-table dao_id
                                                                {
                                                                    "dao_members_count": (- t-member-count 1)
                                                                }
                                                              )
                                                            )
                                                            (with-capability (CAN_UPDATE dao_id)
                                                              (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "Member {} has been voted out of the Hive" [remove-member-id]))
                                                            )
                                                        )
                                                      )

                                                      (update dao-proposals-table proposal_id
                                                        {
                                                            "proposal_completed": true,
                                                            "proposal_completed_time": (at 'block-time (chain-data)),
                                                            "proposal_completed_action": t_action
                                                        }
                                                      )
                                                    ))
                      ((= t_action "AGAINST") (let*
                                                      (
                                                        (AGAINST true)
                                                      )
                                                      (with-capability (CAN_UPDATE dao_id)
                                                        (add-dao-update dao_id dao_id (format "Proposal {} Opposed" [proposal_count]) (format "Proposal {} was opposed!" [proposal_id]))
                                                      )
                                                      (update dao-proposals-table proposal_id
                                                        {
                                                            "proposal_completed": true,
                                                            "proposal_completed_time": (at 'block-time (chain-data)),
                                                            "proposal_completed_action": t_action
                                                        }
                                                      )
                                                    ))
                      ((= t_action "ENABLE_PROPOSAL_CONTROL") (let*
                                                            (
                                                              (ENABLE_PROPOSAL_CONTROL true)
                                                              (dao-data (read daos-table dao_id))
                                                              (dao-locked (at "dao_members_locked" dao-data))
                                                            )
                                                            (if (= dao-locked true)
                                                              (with-capability (CAN_UPDATE dao_id)
                                                                (add-dao-update dao_id dao_id (format "Proposal {} Failed" [proposal_count]) "Proposal Control cannot be enabled for this type of Hive")
                                                              )
                                                              (let
                                                                  (
                                                                    (ENABLE_CONTROL true)
                                                                  )
                                                                  (update daos-table dao_id
                                                                    {
                                                                        "dao_all_can_propose": false
                                                                    }
                                                                  )
                                                                  (with-capability (CAN_UPDATE dao_id)
                                                                    (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) "Proposals can now only be created by members designated by the Hive's creator")
                                                                  )
                                                              )
                                                            )
                                                            (update dao-proposals-table proposal_id
                                                              {
                                                                  "proposal_completed": true,
                                                                  "proposal_completed_time": (at 'block-time (chain-data)),
                                                                  "proposal_completed_action": t_action
                                                              }
                                                            )
                                                          ))
                      ((= t_action "ENABLE_WEIGHT") (let*
                                                            (
                                                              (ENABLE_WEIGHT true)
                                                              (dao-data (read daos-table dao_id))
                                                              (dao-locked (at "dao_members_locked" dao-data))
                                                            )
                                                            (if (= dao-locked true)
                                                              (with-capability (CAN_UPDATE dao_id)
                                                                (add-dao-update dao_id dao_id (format "Proposal {} Failed" [proposal_count]) "Weight Mode cannot be enabled for this type of Hive")
                                                              )
                                                              (let
                                                                  (
                                                                    (ENABLE_WEIGHTS true)
                                                                  )
                                                                  (update daos-table dao_id
                                                                    {
                                                                        "dao_use_weight": true
                                                                    }
                                                                  )
                                                                  (with-capability (CAN_UPDATE dao_id)
                                                                    (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) "The Hive's consensus method has been switched to Weight Mode")
                                                                  )
                                                                  (update dao-proposals-table proposal_id
                                                                    {
                                                                        "proposal_completed": true,
                                                                        "proposal_completed_time": (at 'block-time (chain-data)),
                                                                        "proposal_completed_action": t_action
                                                                    }
                                                                  )
                                                              )
                                                            )
                                                          ))
                      ((= t_action "SET_WEIGHT") (let*
                                                            (
                                                              (SET_WEIGHT true)
                                                              (pool-id (at 0 t_action_strings))
                                                              (new-weight (at 0 t_action_decimals))
                                                            )
                                                            (update dao-pools-table pool-id
                                                              {
                                                                  "pool_weight": new-weight,
                                                                  "pool_use_weight": true
                                                              }
                                                            )
                                                            (with-capability (CAN_UPDATE dao_id)
                                                              (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "The Vault {} has had it's Weight set to {}" [pool-id new-weight]))
                                                            )
                                                            (update dao-proposals-table proposal_id
                                                              {
                                                                  "proposal_completed": true,
                                                                  "proposal_completed_time": (at 'block-time (chain-data)),
                                                                  "proposal_completed_action": t_action
                                                              }
                                                            )
                                                          ))
                      ((constantly true) (let*
                                                      (
                                                        (CUSTOM true)
                                                        (description (at 0 t_action_strings))
                                                      )
                                                      (with-capability (CAN_UPDATE dao_id)
                                                        (add-dao-update dao_id dao_id (format "Proposal {} Passed" [proposal_count]) (format "Proposal {} has passed! - '{}' " [proposal_id description]))
                                                      )
                                                      (update dao-proposals-table proposal_id
                                                        {
                                                            "proposal_completed": true,
                                                            "proposal_completed_time": (at 'block-time (chain-data)),
                                                            "proposal_completed_action": t_action
                                                        }
                                                      )
                                                    ))
                                                    false)
              )
        )
    )


    ;;///////////////////////
    ;;GETTERS
    ;;//////////////////////

    ;Get all dao ids
    (defun get-daos ()
      (keys daos-table)
    )

    ;Get a daos info
    (defun get-dao-info (dao-id:string)
      (read daos-table dao-id)
    )

    ;;Get dao msg
    (defun get-dao-message (dao-id:string count:integer)
      (read dao-messages-table (get-2-key count dao-id))
    )
    ;;Get dao msgs
    (defun get-dao-messages (dao-id:string)
      (let*
          (
              (dao-message-count (at "dao_messages_count" (read daos-table dao-id)))
              (enum-count (enumerate 1 dao-message-count))
          )
          (map (get-dao-message dao-id) enum-count)
      )
    )


    ;;Get dao update
    (defun get-dao-update (dao-id:string count:integer)
      (read dao-updates-table (get-2-key count dao-id))
    )
    ;;Get dao updates
    (defun get-dao-updates (dao-id:string)
      (let*
          (
              (dao-update-count (at "dao_updates_count" (read daos-table dao-id)))
              (enum-count (enumerate 1 dao-update-count))
          )
          (map (get-dao-update dao-id) enum-count)
      )
    )


    ;;Get dao pool
    (defun get-dao-pool (dao-id:string count:integer)
      (read dao-pools-table (get-2-key count dao-id))
    )
    ;;Get dao pools
    (defun get-dao-pools (dao-id:string)
      (let*
          (
              (dao-pool-count (at "dao_pool_count" (read daos-table dao-id)))
              (enum-count (enumerate 1 dao-pool-count))
          )
          (map (get-dao-pool dao-id) enum-count)
      )
    )


    ;;Get dao proposal
    (defun get-dao-proposition (dao-id:string count:integer)
      (let
          (
              (dao-votes (get-proposal-vote-options (get-2-key count dao-id)))
              (dao-propositions (read dao-proposals-table (get-2-key count dao-id)))
          )
          {
            "voting_options": dao-votes,
            "proposition": dao-propositions
          }
       )
    )
    ;;Get dao proposals
    (defun get-dao-propositions (dao-id:string)
      (let*
          (
              (dao-proposition-count (at "dao_proposal_count" (read daos-table dao-id)))
              (enum-count (enumerate 1 dao-proposition-count))
          )
          (map (get-dao-proposition dao-id) enum-count)
      )
    )


    ;;Get dao vote option
    (defun get-proposal-vote-option (proposal-id:string count:integer)
      (read dao-votes-table (get-2-key count proposal-id))
    )
    ;;Get dao vote options
    (defun get-proposal-vote-options (proposal-id:string)
      (let*
          (
              (proposition-options-count (- (at "proposal_options_count" (read dao-proposals-table proposal-id)) 1) )
              (enum-count (enumerate 1 proposition-options-count))
          )
          (map (get-proposal-vote-option proposal-id) enum-count)
      )
    )



    ;;Get dao member
    (defun get-dao-member (dao_id:string count:integer)
      (read dao-accounts-count-table (get-2-key count dao_id))
    )
    ;;Get all dao members by count
    (defun get-all-dao-members (dao-id:string)
      (let*
          (
              (member-count (- (at "dao_members_count" (read daos-table dao-id)) 1) )
              (enum-count (enumerate 0 member-count))
          )
          (map (get-dao-member dao-id) enum-count)
      )
    )


    ;;Get user votes for a proposition
    (defun get-user-proposal-vote (account-id:string proposal-id:string)
      (read user-vote-records (get-user-key account-id proposal-id))
    )

    ;Get dao members via select
    (defun get-dao-members ( dao-id:string )
      (select dao-accounts-table
          (and? (where 'account_dao_id (= dao-id))
            (where 'account_banned (= false))))
    )

    ;Gets a users daos
    (defun get-member-daos ( account:string )
        (let
          (
              (member-daos (at "dao_ids" (read dao-membership-ids-table account)))
          )
          (map (get-dao-info) member-daos)
      )
    )

    ;Gets all daos
    (defun get-all-daos ()
        (let
          (
              (daos (get-daos))
          )
          (map (get-dao-info) daos)
      )
    )

    ;Checks if a user is a member of a dao
    (defun is-member(dao-id:string account:string)
        (let*
            (
              (memberships (read dao-membership-ids-table account))
              (membership-ids (at "dao_ids" memberships))
            )
            (contains dao-id membership-ids)
        )
    )

    ;Checks if a user can propose in a dao
    (defun can-propose(dao-id:string account:string)
        (let*
            (
              (membership (read dao-accounts-table (get-user-key account dao-id)))
              (can-propose (at "account_can_propose" membership))
            )
            can-propose
        )
    )


    ;;///////////////////////
    ;;UTILITIES
    ;;//////////////////////

    (defun create-account-key:string
      ( pool_id:string account_id:string)
      (hash (+ account_id (+ pool_id (format "{}" [(at 'block-time (chain-data))]))))
    )

    (defun get-token-key
      ( tokenA:module{fungible-v2} )
      (format "{}" [tokenA])
    )

    (defun get-2-key ( count:integer subject_id:string )
      (format "{}:{}" [count subject_id])
    )

    (defun get-user-key ( pool_id:string account:string )
      (format "{}:{}" [pool_id account])
    )

    (defun composelist
      (
        stringlist:string
      )
      (let*
        (
          (current:string stringlist)
        )
        current
      )
    )

    (defun enforce-unit:bool (amount:decimal precision)
      @doc " Enforces precision "
      (enforce
        (= (floor amount precision)
           amount)
        "Minimum denomination exceeded.")
    )

  (defun enforce-valid-name ( name:string )
    @doc " Enforce that a Pool Name meets charset and length requirements. "
    (enforce
      (is-charset ACCOUNT_ID_CHARSET name)
      (format
        "Pool Name does not conform to the required charset: {}"
        [name]))
    (let ((nameLength (length name)))
      (enforce
        (>= nameLength NAME_MIN_LENGTH)
        (format
          "Pool Name does not conform to the min length requirement: {}"
          [name]))
      (enforce
        (<= nameLength NAME_MAX_LENGTH)
        (format
          "Pool Name does not conform to the max length requirement: {}"
          [name]))))

  (defun enforce-valid-description ( name:string )
    @doc " Enforce that a Description meets charset and length requirements. "
    (enforce
      (is-charset ACCOUNT_ID_CHARSET name)
      (format
        "Description does not conform to the required charset: {}"
        [name]))
    (let ((nameLength (length name)))
      (enforce
        (>= nameLength DESCRIPTION_MIN_LENGTH)
        (format
          "Description does not conform to the min length requirement: {}"
          [name]))
      (enforce
        (<= nameLength DESCRIPTION_MAX_LENGTH)
        (format
          "Description does not conform to the max length requirement: {}"
          [name]))))

)

; (create-table free.dao-hive-factory8.daos-table)
; (create-table free.dao-hive-factory8.dao-membership-ids-table)
; (create-table free.dao-hive-factory8.dao-messages-table)
; (create-table free.dao-hive-factory8.dao-updates-table)
; (create-table free.dao-hive-factory8.dao-accounts-table)
; (create-table free.dao-hive-factory8.dao-pools-table)
; (create-table free.dao-hive-factory8.dao-proposals-table)
; (create-table free.dao-hive-factory8.dao-votes-table)
; (create-table free.dao-hive-factory8.user-vote-records)
; (create-table free.dao-hive-factory8.user-proposition-records)
; (create-table free.dao-hive-factory8.dao-accounts-count-table)
