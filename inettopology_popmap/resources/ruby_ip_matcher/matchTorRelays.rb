#!/usr/bin/env ruby

$LOAD_PATH.unshift File.dirname(__FILE__)

require 'pry'
require 'logger'
require 'ipaddress'
require 'hiredis'
require 'search'
require 'redis'
require 'ipaddr'
require 'trollop'
require 'rest_client'
require 'json'
$log = Logger.new STDERR

opts = Trollop::options do
  version "matchIPtoPoP v0.0.1 (c) 2012 Henry Tan, updates by Chris Wacek"
  banner <<-EOS
matchIPtoPoP takes a list of Tor relay information, and outputs
information about where the relays attach to the PoP graph 
loaded in the Redis database.

Matching works on a series of tiebreaks: 
  => One matching IP in /24      
  => >1 matching IPs in /24      
      => Single match at some subnet
      => Multiple match at some subnet
          => Closest IP
          => Numerically lower IP

If one of these conditions succeeds in descending order, it's 
considered a match.

Formats:
        This script can either load 'live' data by requesting it from
        Onionoo, or alternately it can use a previously saved summary
        file from Onionoo. 

        Output will be produced as a JSON list of relay objects:
          { 
            ip:         <relay_ip>, 
            match_ip:   <matched_ip_if_any>,
            match_pop:  <matched_pop>,
            match_asn:  <matched_asn>
            match_possibilities: [
              {ip: <match_ip_1>, pop: <match_pop_1> }
              ....
            ]
          }

Usage:
       test [--live | --local <inputfile>] <output_file>

Options:
EOS

  opt :live, "Load the latest data from Onionoo"
  opt :local, "Load JSON from a file previously downloaded from Onionoo",
      :type => :string
  opt :other, "Match IPs from another data file (they should be the first item per line)", :type => :string
  opt :redis, "The host:port combination to use to connect to Redis",
      :default => "localhost:6379"
  opt :v, "Be verbose"
  opt :f, "Force"
end

Trollop::die :live, "Either 'live' or 'local' must be specified" if not (opts[:live] or opts[:local] or opts[:other])
Trollop::die :local, "'local' cannot be specified if 'live' was requested" if opts[:live] and (opts[:local] or opts[:other])
Trollop::die :other, "'other' cannot be specified if 'live' or 'local' was specified" if opts[:other] and (opts[:live] or opts[:local])
Trollop::die :redis, "Invalid format" if (opts[:redis] =~ /[a-zA-Z0-9\.]+:[0-9]+/).nil?
$log.level = Logger::DEBUG if opts[:v]

def segmentIPs(redis)

  ips = redis.smembers("iplist")

#Okay, we're going to load the data, then we're going to group the 
#IP addresses by their /8 cluster
  $log.info "Building /8 searchblocks"
  searcher = Searcher.new $log

  ips.each_with_index do |ip,i|
    #ipaddr = IPAddress::IPv4.new(ip)
    searcher.add ip
    $log.debug "Processed #{i}/#{ips.length} ips" if i % 10000 == 0
  end

  return searcher
end

def loadData(opts)
  if opts[:live]
    response = RestClient.get 'https://onionoo.torproject.org/summary'

    if response.code != 200
      warn "Failed to grab live data [#{response.code}]"
      exit 1
    end

    return JSON.parse response.to_str
  else
    return JSON.parse IO.read(opts[:local])
  end
end

###Prog Start###

redis_warning = <<MSG
Detected that you're connecting to Redis locally.   
This operation tends to take alot of memory, and  
probably shouldn't be run against the local DB. 

Run with '-f' if you know what you're doing.
MSG

opts[:redis] =~ /([a-zA-Z0-9\.]+):([0-9]+)/
if $1 == "localhost" or $1 == "127.0.0.1" 
  unless opts[:f]
    $log.info "#{redis_warning}"
    exit 1
  end
end

redis = Redis.new host: $1, port: $2, timeout: 1000, driver: :hiredis

begin
  redis.ping
rescue
  $log.info "Failed to connect to Redis"
  exit 1
end

$log.info "Loading IPs from Redis"
searcher = segmentIPs(redis)

stats = {matched: 0, unmatched_pop: 0, unmatched_ip: 0, total: 0}

if not opts[:other]
  data = loadData opts
  relays = data['relays'].select {|relay| relay['r'] }

  puts "["
  relays.each_with_index do |relay,i|

     #'a' contains an array of IPv4 or IPv6 addresses
    stats[:total] += 1
    relay['a'].each do |addr|
      begin
        relay_addr = IPAddress::IPv4.new addr
        next if not relay_addr # this isn't an IPv4 address
      rescue
        next
      end
      $log.info("Searching for match for #{relay['n']} @#{relay_addr.to_s} [#{i}/#{data['relays'].length}")

      match = searcher.search_slash16(relay_addr) || searcher.search_slash8(relay_addr)
      if match.nil?
        $log.warn "No match found for #{addr} even at /8"
        stats[:unmatched_ip] += 1
        next
      end

      $log.info "Found match for #{addr}: #{match.ip} at #{match.bits} bits"
      popInfo = {ip: match.ip, 
                 pop: redis.hget("ip:#{match.ip.to_s}",'pop'), 
                 asn: redis.hget("ip:#{match.ip.to_s}",'asn') 
                }

      if popInfo[:pop].nil?
        $log.info "No pop for #{match.ip} available. Will not be included in output."
        stats[:unmatched_pop] += 1
        next
      end
      popInfo[:nick] = relay['n']
      popInfo[:fp] = relay['f']
      popInfo[:relay_ip] = addr
      popInfo[:match_bits] = match.bits
      begin
        print(JSON.generate(popInfo))
        print(",\n") if i != relays.length - 1
      rescue
        next
      end
      stats[:matched] += 1
      # break out if we've found all the relays we need to find.
      break

    end
  end
  puts "]"
else
  i = 0
	
	total_lines = File.readlines(opts[:other]).size

  File.open(opts[:other], "r").each_line do |line|
    i += 1
    ip = IPAddress::IPv4.new line.split()[0]
    $log.info("Searching for match for #{ip} [#{i}]")

    match = searcher.search_slash16(ip) || searcher.search_slash8(ip)
		popInfo = {ip: match.ip,
							pop: redis.hget("ip:#{match.ip_to_s}", 'pop')
							}
    if match.nil?
      $log.warn "No match found for #{ip} even at /8"
      stats[:unmatched_ip] += 1
      next
    end

		popInfo[:relay_ip] = ip
		popInfo[:match_bits] = match.bits
	
    $log.info "Found match for #{ip}: #{match.ip} at #{match.bits} bits"

#    print "#{line} #{match.ip} #{match.bits}"
    stats[:matched] += 1

    begin
    	print(JSON.generate(popInfo))
    	print(",\n") if i != total_lines - 1
    rescue
    	next
    end

  end
end

$log.info <<-endmsg
# relays processed:     #{stats[:total]}
# matches found:        #{stats[:matched]}
# unmatched:            #{stats[:unmatched_ip]}
# matched, missing pop: #{stats[:unmatched_pop]}
endmsg

