require "../version"
require "../type"
require "../pack_stream"
require "../pack_stream/packer"
require "../result"
require "./transaction"
require "./from_bolt"

require "socket"
require "openssl"

module Neo4j
  module Bolt
    class Connection
      GOGOBOLT = "\x60\x60\xB0\x17".to_slice
      SUPPORTED_VERSIONS = { 2, 0, 0, 0 }
      enum Commands
        Init       = 0x01
        Run        = 0x10
        PullAll    = 0x3F
        AckFailure = 0x0E
        Reset      = 0x0F
      end

      @connection : (TCPSocket | OpenSSL::SSL::Socket::Client)
      @transaction : Transaction?

      def initialize
        initialize "bolt://neo4j:neo4j@localhost:7687", ssl: false
      end

      # Initializes this connection with the given URL string and SSL flag.
      #
      # ```
      # connection = Neo4j::Bolt::Connection.new("bolt://neo4j:password@localhost", ssl: false)
      # ```
      def initialize(url : String, ssl : Bool = true)
        initialize URI.parse(url), ssl
      end

      # Initializes this connection with the given URI and SSL flag. SSL
      # defaults to `true` so that, if you omit it by mistake, you aren't
      # sending your database credentials in plaintext.
      #
      # ```
      # uri = URI.parse("bolt://neo4j:password@localhost")
      # connection = Neo4j::Bolt::Connection.new(uri, ssl: false)
      # ```
      def initialize(@uri : URI, @ssl=true, connection_retries = 5)
        host = uri.host.to_s
        port = uri.port || 7687
        username = uri.user.to_s
        password = uri.password.to_s

        if uri.scheme != "bolt"
          raise ArgumentError.new("Connection must use Bolt")
        end

        @connection = loop do
                        break TCPSocket.new(host, port)
                      rescue ex
                        connection_retries -= 1
                        if connection_retries < 0
                          raise ex
                        end
                      end

        if ssl
          context = OpenSSL::SSL::Context::Client.new
          context.add_options(OpenSSL::SSL::Options::NO_SSL_V2 | OpenSSL::SSL::Options::NO_SSL_V3)

          # Must set hostname for SNI (Server Name Indication) on some platforms, such as Neo4j Aura
          @connection = OpenSSL::SSL::Socket::Client.new(@connection, context, hostname: uri.host)
        end

        handshake

        init username, password
      end

      # Returns a streaming iterator that consumes results lazily from the
      # Neo4j server. This can be used when the result set is very large so that
      # your application does not need to retain the full result set in memory.
      #
      # ```
      # results = connection.stream(query, user_id: user.id)
      #
      # results.each do |(user)|
      #   process User.new(user.as(Neo4j:Node))
      # end
      # ```
      #
      # This example yields each result as it comes back from the database, but
      # makes the query metadata available immediately.
      #
      # *NOTE:* You can only consume these results once. Anything that calls
      #   `Enumerable#each(&block)` will fully consume all results. This means
      #   calls like `Enumerable#first` and `Enumerable#size` are destructive.
      #   This is a side effect of using the streaming iterator.
      #
      # *NOTE:* If you are using a connection pool, you *must* consume all of
      #   the results before the connection goes back into the pool. Otherwise,
      #   the connection will be in an inconsistent state and you will need to
      #   manually `Neo4j::Bolt::Connection#reset` it.
      #
      # ```
      # pool.connection do |connection|
      #   results = connection.stream(query)
      #   results.each do |(user)|
      #     process User.new(user.as Neo4j::Node)
      #   end
      # ensure
      #   results.each {} # Finish consuming the results
      # end
      # ```
      def stream(_query, **parameters)
        stream(_query, parameters.to_h.transform_keys(&.to_s))
      end

      # Returns a streaming iterator that consumes results lazily from the
      # Neo4j server. This can be used when the result set is very large so that
      # your application does not need to retain the full result set in memory.
      #
      # ```
      # results = connection.stream(query, Neo4j::Map { "user_id" => user.id })
      #
      # results.each do |(user)|
      #   process User.new(user.as(Neo4j:Node))
      # end
      # ```
      #
      # This example yields each result as it comes back from the database, but
      # makes the query metadata available immediately.
      #
      # *NOTE:* You can only consume these results once. Anything that calls
      #   `Enumerable#each(&block)` will fully consume all results. This means
      #   calls like `Enumerable#first` and `Enumerable#size` are destructive.
      #   This is a side effect of using the streaming iterator.
      #
      # *NOTE:* If you are using a connection pool, you *must* consume all of
      #   the results before the connection goes back into the pool. Otherwise,
      #   the connection will be in an inconsistent state and you will need to
      #   manually `Neo4j::Bolt::Connection#reset` it.
      #
      # ```
      # pool.connection do |connection|
      #   results = connection.stream(query)
      #   results.each do |(user)|
      #     process User.new(user.as Neo4j::Node)
      #   end
      # ensure
      #   results.each {} # Finish consuming the results
      # end
      # ```
      def stream(query, parameters = Map.new)
        StreamingResult.new(
          type: run(query, parameters),
          data: stream_results,
        )
      end

      private def stream_results
        send Commands::PullAll
        results = StreamingResultSet.new

        result = read_result
        case result
        when Success, Ignored
          results.complete!
        else
          spawn do
            until result.is_a?(Success) || result.is_a?(Ignored)
              results << result.as(List)
              result = read_result
            end
            results.complete!
          end
        end

        results
      end

      def execute(_query, **parameters, &block : List ->)
        params_hash = Map.new

        parameters.each do |key, value|
          params_hash[key.to_s] = value.to_bolt_params
        end

        execute _query, params_hash, &block
      end

      # Executes the given query with the given parameters and executes the
      # block once for each result returned from the database.
      #
      # ```
      # connection.execute <<-CYPHER, Neo4j::Map { "id" => 123 } do |(user)|
      #   MATCH (user:User { id: $id })
      #   RETURN user
      # CYPHER
      #   process User.new(user.as Neo4j::Node)
      # end
      # ```
      #
      # In the example above, we used `()` in the block arguments to destructure
      # the list of `RETURN`ed values. Also note that we need to cast it down to
      # a `Neo4j::Node`. All values in results have the compile-time type of
      # `Neo4j::Value` and so will need to be cast down to its specific type.
      def execute(query, parameters : Map, &block : List ->)
        send Commands::Run, query, parameters
        send Commands::PullAll

        response = read_result.as(Response) # RUN
        handle_result response
        result = read_result
        until result.is_a?(Neo4j::Response)
          yield result.as(List)
          result = read_result
        end

        {response, result.as(Response)}
      end

      # Execute the given query with the given parameters, returning a Result
      # object containing query metadata and the query results in an array.
      #
      # ```
      # connection.execute(query, Neo4j::Map { "id" => 123 })
      # ```
      def execute(query, parameters : Map)
        Result.new(type: run(query, parameters), data: pull_all)
      rescue e
        reset unless @transaction # Let the transaction handle this
        raise e
      end

      # Execute the given query with the given parameters, returning a Result
      # object containing query metadata and the query results in an array.
      #
      # This method provides a convenient shorthand for providing a `Neo4j::Map`
      # of query parameters.
      #
      # ```
      # connection.execute(query, id: 123)
      # ```
      def execute(_query query, **params)
        params_hash = Map.new

        params.each { |key, value| params_hash[key.to_s] = value.to_bolt_params }

        execute query, params_hash
      end

      def exec_cast(query : String, types : Tuple(*TYPES)) forall TYPES
        exec_cast query, Map.new, types
      end

      # Execute the given query with the given parameters, returning an array
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      # connection.exec_cast(<<-CYPHER, { email: "me@example.com" }, {User})
      #   MATCH (user:User { email: $email })
      #   RETURN user
      # CYPHER
      # ```
      def exec_cast(query : String, parameters : NamedTuple, types : Tuple(*TYPES)) forall TYPES
        params = Neo4j::Map.new
        parameters.each do |key, value|
          params[key.to_s] = value.to_bolt_params
        end
        exec_cast query, params, types
      end

      # Execute the given query with the given parameters, yielding a tuple
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      # connection.exec_cast(<<-CYPHER, { email: "me@example.com" }, {User}) do |(user)|
      #   MATCH (user:User { email: $email })
      #   RETURN user
      # CYPHER
      #   process user
      # end
      # ```
      def exec_cast(query : String, parameters : NamedTuple, types : Tuple(*TYPES), &block) forall TYPES
        params = Neo4j::Map.new
        parameters.each { |key, value| params[key.to_s] = value.to_bolt_params }
        exec_cast query, params, types do |row|
          yield row
        end
      end

      # Execute the given query with the given parameters, yielding a tuple
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      # connection.exec_cast(<<-CYPHER, Neo4j::Map { "email" => "me@example.com" }, {User}) do |(user)|
      #   MATCH (user:User { email: $email })
      #   RETURN user
      # CYPHER
      #   process user
      # end
      # ```
      def exec_cast(query : String, parameters : Map, types : Tuple(*TYPES), &block) : Nil forall TYPES
        send Commands::Run, query, parameters
        send Commands::PullAll

        query_result = read_result.as(Response)
        result = read_raw_result
        error = nil
        if query_result.is_a? Failure
          error = ::Neo4j::QueryException.new(query_result.attrs["message"].as(String), query_result.attrs["code"].as(String))
        end

        until result[1] != 0x71
          unless error
            # First 3 bytes are Structure, Record, and List
            # TODO: If the RETURN clause in the query has more than 16 items,
            # this will break because the List byte marker and its size won't be
            # in a single byte. We'll need to detect this here.

            io = IO::Memory.new(result + 3)
            begin
              yield types.from_bolt(io)
            rescue e
              error = e unless error
            end
          end

          result = read_raw_result
        end

        if error
          ack_failure
        end

        handle_result query_result

        if error
          raise error
        end
      end

      def exec_cast(query : String, types : Tuple(*TYPES), &block) : Nil forall TYPES
        exec_cast query, Neo4j::Map.new, types do |row|
          yield row
        end
      end

      # Execute the given query with the given parameters, returning an array
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      # connection.exec_cast(<<-CYPHER, Neo4j::Map { "email" => "me@example.com" }, {User})
      #   MATCH (user:User { email: $email })
      #   RETURN user
      # CYPHER
      # # => [{User(@id="4478440e-1897-41a9-812d-91f6d21b994b", @email="me@example.com")}]
      # ```
      def exec_cast(query : String, parameters : Map, types : Tuple(*TYPES)) forall TYPES
        {% begin %}
          results = Array({{ TYPES.type_vars.map(&.stringify.gsub(/\.class$/, "").id).stringify.tr("[]", "{}").id }}).new

          exec_cast query, parameters, types do |row|
            results << row
          end

          results
        {% end %}
      end

      # Execute the given query with the given parameters, returning an array
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      # connection.exec_cast_single(<<-CYPHER, Neo4j::Map { "email" => "me@example.com" }, {User})
      #   MATCH (user:User { email: $email })
      #   RETURN user
      #   LIMIT 1
      # CYPHER
      # # => {User(@id="4478440e-1897-41a9-812d-91f6d21b994b", @email="me@example.com")}
      # ```
      def exec_cast_single(query : String, parameters : Map, types : Tuple(*TYPES)) forall TYPES
        handled = false
        result = nil
        exec_cast query, parameters, types do |row|
          unless handled
            result = row
            handled = true
          end
        end

        result.not_nil!
      end

      # Execute the given query with the given parameters, returning a single
      # result cast to the given type.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      # connection.exec_cast_scalar(<<-CYPHER, Neo4j::Map { "email" => "me@example.com" }, User)
      #   MATCH (user:User { email: $email })
      #   RETURN user
      #   LIMIT 1
      # CYPHER
      # # => User(@id="4478440e-1897-41a9-812d-91f6d21b994b", @email="me@example.com")
      # ```
      def exec_cast_scalar(query : String, parameters : Map, type : T) forall T
        exec_cast_single(query, parameters, {type}).first
      end

      def exec_cast_scalar(query : String, type : T) forall T
        exec_cast_single(query, Neo4j::Map.new, {type}).first
      end

      # Wrap a group of queries into an atomic transaction. Yields a
      # `Neo4j::Bolt::Transaction`.
      #
      # ```
      # connection.transaction do |txn|
      #   connection.execute query1
      #   connection.execute query2
      # end
      # ```
      #
      # Exceptions raised within the block will roll back the transaction. To
      # roll back the transaction manually and exit the block, call
      # `txn.rollback`.
      def transaction
        if @transaction
          raise NestedTransactionError.new("Transaction already open, cannot open a new transaction")
        end

        @transaction = Transaction.new(self)

        execute "BEGIN"
        yield(@transaction.not_nil!).tap { execute "COMMIT" }
      rescue e : RollbackException
        execute "ROLLBACK"
      rescue e : NestedTransactionError
        # We don't want our NestedTransactionError to be picked up by the
        # catch-all rescue below, so we're explicitly capturing and re-raising
        # here to bypass it
        reset
        raise e
      rescue e
        execute "ROLLBACK"
        reset
        raise e
      ensure
        @transaction = nil
      end

      def close
        @connection.close
      end

      # If the connection gets into a wonky state, this method tells the server
      # to reset it back to a normal state, but you lose everything you haven't
      # pulled down yet.
      def reset
        send Commands::Reset
        read_result
      end

      private def ack_failure
        send Commands::AckFailure
        read_result
      end

      private def init(username, password)
        send Commands::Init, "Neo4j.cr/#{VERSION}", {
          "scheme" => "basic",
          "principal" => username,
          "credentials" => password,
        }

        case result = read_result
        when Success
          result
        when Failure
          raise Exception.new(result.inspect)
        else
          raise Exception.new("Don't know how to handle result: #{result.inspect}")
        end
      end

      private def write_message
        packer = PackStream::Packer.new
        yield packer

        total_slice = packer.to_slice
        length = total_slice.size

        message = IO::Memory.new.tap { |io|
          offset = 0
          while length - offset > 0
            slice = total_slice[offset, {(length - offset), 0xFFFF}.min]
            io.write_bytes(slice.size.to_u16, IO::ByteFormat::BigEndian)
            io.write slice
            offset += 0xFFFF
          end
          io.write_bytes 0x0000.to_u16, IO::ByteFormat::BigEndian
        }.to_slice
        @connection.write message
        @connection.flush
      end

      private def run(statement, parameters = Map.new, retries = 5)
        send Commands::Run, statement, parameters

        result = read_result
        case result
        when Failure
          handle_result result
        when Success, Ignored
          result
        else
          raise ::Neo4j::UnknownResult.new("Cannot identify this result: #{result.inspect}")
        end
      rescue ex : IO::Error | OpenSSL::SSL::Error
        if retries > 0
          initialize @uri, @ssl
          run statement, parameters, retries - 1
        else
          raise ex
        end
      end

      private def send(command : Commands, *fields)
        write_message do |msg|
          msg.write_structure_start fields.size
          msg.write_byte command
          fields.each do |field|
            msg.write field
          end
        end
      end

      private def pull_all(&block) : Nil
        send Commands::PullAll

        result = read_result

        until result.is_a?(Success) || result.is_a?(Ignored)
          yield result.as(List)
          result = read_result
        end
      end

      private def pull_all : Array(List)
        results = Array(List).new
        pull_all { |result| results << result }

        results
      end

      private def read_result
        PackStream.unpack(read_raw_result)
      end

      EXCEPTIONS = {
        "Neo.ClientError.Schema.IndexAlreadyExists" => IndexAlreadyExists,
        "Neo.ClientError.Schema.ConstraintValidationFailed" => ConstraintValidationFailed,
        "Neo.ClientError.Statement.ParameterMissing" => ParameterMissing,
        "Neo.ClientError.Statement.SyntaxError" => SyntaxError,
      }
      private def handle_result(result : Failure)
        exception_class = EXCEPTIONS[result.attrs["code"].as(String)]? || QueryException
        raise exception_class.new(
          result.attrs["message"].as(String | Nil).to_s,
          result.attrs["code"].as(String),
        )
      end

      private def handle_result(result : Ignored)
      end

      private def handle_result(result : Success)
      end

      # Read a single result from the server in 64kb chunks. This is a bit of a
      # naive implementation that buffers the chunks until it has the full
      # result, then flattens out the chunks into a single slice and parses
      # that slice. 
      private def read_raw_result
        length = 1_u16
        messages = [] of Bytes
        length = @connection.read_bytes(UInt16, IO::ByteFormat::BigEndian)
        while length != 0x0000
          bytes = Bytes.new(length)
          @connection.read_fully bytes
          messages << bytes
          length = @connection.read_bytes(UInt16, IO::ByteFormat::BigEndian)
        end

        bytes = Bytes.new(messages.map(&.size).sum)
        current_byte = 0
        messages.reduce(bytes) do |chunk, msg|
          msg.copy_to chunk
          current_byte += msg.size

          bytes + current_byte
        end

        bytes
      end

      private def write(value)
        @connection.write_bytes value, IO::ByteFormat::BigEndian
        @connection.flush
      end

      private def write_value(value)
        @connection.write PackStream.pack(value)
      end

      private def handshake
        @connection.write GOGOBOLT
        SUPPORTED_VERSIONS.each do |ver|
          @connection.write_bytes ver, IO::ByteFormat::BigEndian
        end
        @connection.flush

        # Read server version
        # @todo Verify this
        @connection.read_bytes(
          Int32,
          IO::ByteFormat::BigEndian
        )
      end

      private def send_message(string : String)
        send_message string.to_slice
      end

      private def send_message(bytes : Bytes)
        @connection.write bytes
      end

      private def retry(times)
        loop do
          return yield
        rescue ex : IO::Error | OpenSSL::SSL::Error
          if times > 0
            initialize @uri, @ssl
            times -= 1
          else
            raise ex
          end
        end
      end
    end
  end

  class StreamingResultSet
    include Iterable(List)

    delegate complete!, to: @iterator

    def initialize
      @iterator = Iterator.new
    end

    def <<(value : List) : self
      @iterator.channel.send value
      self
    end

    def each
      @iterator
    end

    class Iterator
      include ::Iterator(List)

      getter channel

      def initialize
        @channel = Channel(List).new(1)
        @stop_channel = Channel(Nil).new(1)
        @complete = false
      end

      def next
        return stop if @complete

        select
        when value = @channel.receive
          value
        when @stop_channel.receive
          stop
        end
      end

      def complete!
        @stop_channel.send nil
        @complete = true
      end
    end
  end

  class StreamingResult
    include Iterable(List)
    include Enumerable(List)

    getter type, data

    def initialize(
      @type : Success | Ignored,
      @data : StreamingResultSet,
    )
    end

    def each
      @data.each
    end

    def each(&block : List ->)
      each.each do |row|
        yield row
      end
    end
  end
end
