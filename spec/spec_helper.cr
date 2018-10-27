require "spec"

module Spec
  class PendingExample
    getter(
      description : String,
      file : String,
      line : Int32,
      end_line : Int32,
      block : Proc(Nil),
      channel : Channel(Exception?),
    )

    def initialize(@description, @file, @line, @end_line, @block)
      @channel = Channel(Exception?).new
    end

    def start
      spawn do
        it description, file, line, end_line do
          block.call
        end
        channel.send nil
      end
    end

    def wait
      exception = channel.receive
    end
  end

  @@pending_examples = Array(PendingExample).new

  def self.pending_examples
    @@pending_examples
  end

  def self.run
    start_time = Time.monotonic
    at_exit do
      continue = true
      Signal::INT.trap { continue = false }
      pending_examples.each do |example|
        break unless continue
        example.wait
      end
      elapsed_time = Time.monotonic - start_time
      Spec::RootContext.finish(elapsed_time, @@aborted)
      exit 1 unless Spec::RootContext.succeeded && !@@aborted
    end
  end

  module Methods
    def async_it(description = "assert", file = __FILE__, line = __LINE__, end_line = __END_LINE__, &block)
      example = PendingExample.new(
        description: description,
        file: file,
        line: line,
        end_line: end_line,
        block: block,
      )

      Spec.pending_examples << example
      example.start
    end
  end
end
