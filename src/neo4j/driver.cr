module Neo4j
  abstract class Driver
    abstract def session(& : Session ->)

    # abstract def write_query(query : String, **params)
    # abstract def write_query(query : String, as types : Tuple(*T), **params, &) forall T
    # abstract def read_query(query : String, as types : Tuple(*T), **params, &) forall T

    def write_query(query : String, as types : Tuple(*T), **params) forall T
      results = [] of Tuple(*T)

      write_query query, types, **params do |result|
        results << result
      end

      results
    end

    def read_query(query : String, as types : Tuple(*T), **params) forall T
      {% begin %}
        results = Array(Tuple({{T.type_vars.map(&.instance).join(", ").id}})).new

        read_query query, types, **params do |result|
          results << result
        end

        results
      {% end %}
    end
  end
end
