class Redis
  module Ext
    # Hack to override instance methods in Redis::Client
    # (from: http://gist.github.com/499673)
    def self.included(klass)
      (klass.instance_methods & InstanceMethods.instance_methods).each do |method|
        klass.instance_eval { remove_method method.to_sym }
      end
      klass.send(:include, InstanceMethods)
    end

    module InstanceMethods

      def connection
        @connection ||= ::RedisExt::Connection.new
      end

      def connected?
        connection.connected?
      end

      def disconnect
        return unless connected?
        connection.disconnect
      end

      def read
        begin
          connection.read
        rescue Errno::EAGAIN

          # We want to make sure it reconnects on the next command after the
          # timeout. Otherwise the server may reply in the meantime leaving
          # the protocol in a desync status.
          disconnect

          raise Errno::EAGAIN, "Timeout reading from the socket"
        rescue ::RedisExt::Connection::EOFError
          raise Errno::ECONNRESET, "Connection lost"
        end
      end

      def write(commands)
        commands.each { |c| connection.write c }
      end

    protected

      def connect_to(host, port)
        with_timeout(@timeout) do
          connection.connect(host, port)
        end

        # If the timeout is set we set the low level socket options in order
        # to make sure a blocking read will return after the specified number
        # of seconds. This hack is from memcached ruby client.
        self.timeout = @timeout

      rescue Errno::ECONNREFUSED
        raise Errno::ECONNREFUSED, "Unable to connect to Redis on #{host}:#{port}"
      end

      def timeout=(timeout)
        # tbd
      end

    end
  end
end
