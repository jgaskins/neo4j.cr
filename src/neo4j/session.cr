require "./bolt/transaction"

module Neo4j
  abstract class Session
    abstract def write_transaction(& : Neo4j::Bolt::Transaction -> T) : T forall T
    abstract def read_transaction(& : Neo4j::Bolt::Transaction -> T) : T forall T
    abstract def close
    abstract def closed?
  end
end
