require "./bolt/transaction"

module Neo4j
  abstract class Session
    abstract def write_transaction(& : Neo4j::Bolt::Transaction -> T) forall T
    abstract def read_transaction(& : Neo4j::Bolt::Transaction -> T) forall T
    abstract def exec_cast(query : String, as types : Tuple(*T), **params) forall T
  end
end
