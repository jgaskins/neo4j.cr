require "../../../spec_helper"

module Neo4j
  module PackStream
    macro unpack(value)
      Unpacker.new({{value}}).read
    end

    describe Unpacker do
      it "unpacks ints" do
        unpack("\x7A").should eq 0x7A
        unpack("\xC8\x20").should eq 0x20
        unpack("\xC9\x12\x34").should eq 0x1234
        unpack("\xCA\x12\x34\x56\x78").should eq 0x1234_5678
      end

      it "unpacks strings" do
        unpack("\x85Jamie").should eq "Jamie"
        unpack("\xD0\x0DJamie Gaskins").should eq "Jamie Gaskins"
      end

      it "unpacks nil" do
        unpack("\xC0").should eq nil
      end

      it "unpacks booleans" do
        unpack("\xC2").should eq false
        unpack("\xC3").should eq true
      end

      it "unpacks arrays" do
        unpack("\x90").should eq [] of Neo4j::Type
        unpack("\x91\x85Jamie").should eq ["Jamie"]
      end

      it "unpacks hashes" do
        unpack("\xA0").should eq({} of String => Neo4j::Type)
        unpack("\xA1\x84name\x85Jamie").should eq({ "name" => "Jamie" })
      end

      it "unpacks structures" do
        # 0x70 is the byte marker for a Success result
        structure = unpack("\xB1\x70\xA0").as(Neo4j::Success)

        structure.attrs.should eq({} of String => Neo4j::Type)
      end
    end
  end
end
