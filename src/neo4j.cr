require "uri"

require "./neo4j/type"
require "./neo4j/mapping"
require "./neo4j/bolt/connection"
require "./neo4j/direct_driver"
require "./neo4j/cluster"
require "./neo4j/driver"

module Neo4j
  def self.connect(uri : URI, ssl : Bool = true) : Driver
    if uri.scheme == "bolt+routing" || uri.scheme == "neo4j"
      Cluster.new(uri, ssl)
    else
      DirectDriver.new(uri, ssl)
    end
  end
end
