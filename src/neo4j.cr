require "uri"

require "./neo4j/type"
require "./neo4j/mapping"
require "./neo4j/bolt/connection"
require "./neo4j/direct_driver"
require "./neo4j/cluster"
require "./neo4j/driver"
require "./neo4j/serializable"

module Neo4j
  def self.connect(uri : URI, ssl : Bool = true) : Driver
    case uri.scheme
    when "bolt+routing", "neo4j"
      Cluster.new(uri, ssl)
    when "neo4j+s"
      Cluster.new(uri, true)
    else
      DirectDriver.new(uri, ssl)
    end
  end
end
