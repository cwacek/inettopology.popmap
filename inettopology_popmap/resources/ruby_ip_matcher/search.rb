
class IPMatch

  attr_reader :ip, :bits

  def initialize(ip,bits)
    @ip = ip
    @bits = bits
  end
end

class Searcher

  class PotentialMatch
    attr_reader :dist, :ip, :matched_with

    def initialize(ip, matched_with)
      @ip = ip
      @matched_with = matched_with
      @dist = ip - matched_with
    end

  end

  def initialize(log)
    @log = log 
    @slash_8s = Hash.new(Hash.new)
  end

  def add(ipaddr)
    ip = ipaddr.instance_of?(IPAddress::IPv4) ? ipaddr : IPAddress::IPv4.new(ipaddr) 
    return if ip.nil?

    unless @slash_8s[ip[0]].has_key? ip[1]
      @slash_8s[ip[0]][ip[1]] = Array.new
    end

    @slash_8s[ip[0]][ip[1]] << ip
  end

  def add_from(iplist)
    iplist.each do |ip|
      self.add ip
    end
  end

  def search_slash8( addr)
    @log.debug("Searching for #{addr} in #{addr[0]}.0.0.0/8")
    slash8_blocks = @slash_8s[addr[0]]
    subnets = IPAddress::IPv4.new("#{addr[0]}.0.0.1/8").subnet(16)

    bits = 16
    while bits > 8
      bits -= 1
      searchnet = addr.supernet(bits)

      possibles = subnets.select { |sn| searchnet.include? sn }
      next if possibles.empty?

      possible_matches = Array.new
      possibles.map do |p_ip|
        next if not slash8_blocks.include? p_ip[1]
        slash8_blocks[p_ip[1]].each do |ip|
          possible_matches << PotentialMatch.new(ip, addr)
        end
      end

      possible_matches.sort! {|x,y| x.dist <=> y.dist} 
      return IPMatch.new possible_matches[0].ip, bits

    end
    return nil
  end

  def search_slash16( addr, iplist = nil)
    iplist ||= @slash_8s[addr[0]][addr[1]]
    return nil if iplist.nil?
    @log.debug "Searching #{addr[0]}.#{addr[1]}.0.0/16, which contains #{iplist.length} ips"

    bits = 32
    while bits > 16
      bits -= 1
      addr_net = addr.supernet(bits)
      masked = iplist.select do |candidate|
        true if addr_net.include? candidate
      end

      next if masked.empty?

      masked.sort! {|x,y| addr - x <=> addr - y}

      return IPMatch.new(masked[0],bits)
    end
    return nil
  end

end
