require "../spec_helper"
require "../../src/neo4j/type"

module Neo4j
  describe Duration do
    it "can be added to timestamps" do
      timestamp = Time.utc(
        year: 2019,
        month: 6,
        day: 30,
        hour: 18,
        minute: 52,
        second: 30,
        nanosecond: 123_456_789,
      )

      duration = Duration.new(
        years: 1,
        months: 1,
        weeks: 1,
        days: 1,
        hours: 1,
        minutes: 1,
        seconds: 1,
        milliseconds: 1,
        microseconds: 1,
        nanoseconds: 1,
      )

      (timestamp + duration).should eq Time.utc(
        year: 2020,
        month: 8,
        day: 8,
        hour: 19,
        minute: 53,
        second: 31,
        nanosecond: 124_457_790,
      )

      (timestamp - duration).should eq Time.utc(
        year: 2018,
        month: 5,
        day: 22,
        hour: 17,
        minute: 51,
        second: 29,
        nanosecond: 122_455_788,
      )
    end
  end
end
