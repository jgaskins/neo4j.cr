module Neo4j
  abstract class Driver
    abstract def session(& : Session ->)
  end
end
