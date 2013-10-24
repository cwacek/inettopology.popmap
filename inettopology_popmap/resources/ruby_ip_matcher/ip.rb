

class IP

  #def initialize
  #  @ip = ['0','0','0','0']
  #end
  
  def initialize(ipText)
    @ip = [nil,nil,nil,nil]
    #if ipText[0..3].eql? 'ip:'
    #  ipText[0..3] = ''
    #end

    if (ipText.match 'ip:')
      text=ipText.slice(3..-1)
    else
      text=ipText
    end
    self.ip=text
  end
  
  def ip=(ipText)
    ipArray = ipText.split('.')
    ipArray.each_index{ |i|
      @ip[i] = ipArray[i]
    }
  end

  def ip
    return @ip
  end
  
  def ipText
    return @ip.join('.')
  end
  
  def redisX24Key
    return 'ip:' + @ip[0..2].join('.') + '.*'
  end
  
  def s_x24
    return 's_ip:' + @ip[0..2].join('.')
  end

  def redis
    return 'ip:' + @ip[0..3].join('.')
  end

  def lastOctet
    return @ip[3]
  end

  def self.newMultiple(ipArray)
    retVal = Array.new
    ipArray.each {|ip|
      retVal << IP.new(ip)                  
    }
    return retVal
  end
  
  
  #returns the smalles xNN subnet which includes this ip and destIP
  #n.b. larger values are smaller subnets
  def getMinSubnet(destip)
    last = @ip[3]
    last2 = destip.lastOctet
    return (32 - (last.to_i^last2.to_i).to_s.length) #largest significant bit which is different is preserved and determines the length
  end

  def getNDiff(destip)
    last = @ip[3]
    last2 = destip.lastOctet
    return (last2.to_i-last.to_i).abs
  end

  def self.isIP(inTxt)
    if inTxt.match(/^ip:/)
      text = inTxt.slice(3..-1)
    else
      text = inTxt.clone
    end

    #text = inTxt.clone
    #text.slice!(3..-1) if text.match('ip:')
    return text.match(/^(?:\d{1,3}\.){3}\d{1,3}$/)
  end

end
