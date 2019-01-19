require "../../../spec_helper"

require "../../../../src/neo4j/pack_stream"

module Neo4j
  module PackStream
    describe Packer do
      it "serializes hashes over 4 bits in length" do
        hash = {
          key_00: "value",
          key_01: "value",
          key_02: "value",
          key_03: "value",
          key_04: "value",
          key_05: "value",
          key_06: "value",
          key_07: "value",
          key_08: "value",
          key_09: "value",
          key_0A: "value",
          key_0B: "value",
          key_0C: "value",
          key_0D: "value",
          key_0E: "value",
          key_0F: "value",
          key_10: "value",
        }.to_h.transform_keys(&.to_s)

        unpack(pack(hash)).should eq hash
      end
    end
  end
end
