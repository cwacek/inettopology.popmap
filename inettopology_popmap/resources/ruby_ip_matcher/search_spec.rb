require 'search'
require 'ipaddress'
require 'pry'

describe Searcher, "#search_slash8" do

  before :each do 
    ips = ["192.168.5.12","192.168.5.14","192.168.5.128", "192.168.5.13"]
    @searcher = Searcher.new
    @searcher.add_from ips.map {|ip| IPAddress::IPv4.new ip }
  end

  describe '#search_slash16' do
    it "finds 192.168.5.12 for 192.168.5.12"  do
      match = @searcher.search_slash16 IPAddress::IPv4.new "192.168.5.12"
      match.should_not eql nil
      match.should be_an_instance_of IPMatch
      match.ip.should == IPAddress::IPv4.new("192.168.5.12")
    end

    it "finds 192.168.5.12 for 192.168.5.11" do
      match = @searcher.search_slash16 IPAddress::IPv4.new "192.168.5.11"
      match.should_not eql nil
      match.should be_an_instance_of IPMatch
      match.ip.should == IPAddress::IPv4.new("192.168.5.12")
    end

    it "finds 192.168.5.128 for 192.168.6.12" do
      match = @searcher.search_slash16 IPAddress::IPv4.new "192.168.6.12"
      match.should_not eql nil
      match.should be_an_instance_of IPMatch
      match.ip.should == IPAddress::IPv4.new("192.168.5.128")
    end

    it "finds 192.168.5.128 for 192.169.6.12" do
      match = @searcher.search_slash16 IPAddress::IPv4.new "192.169.6.12"
      match.should eql nil
      match = @searcher.search_slash8 IPAddress::IPv4.new "192.169.6.12" 
      match.should_not eql nil
      match.should be_an_instance_of IPMatch
      match.ip.should == IPAddress::IPv4.new("192.168.5.128")
    end
  end

end
