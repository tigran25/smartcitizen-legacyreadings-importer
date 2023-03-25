require 'rubygems'
require 'bundler'
Bundler.require

require 'active_record'
require_relative 'mathematician'
require 'socket'

Figaro.application = Figaro::Application.new(environment: "production", path: "application.yml")
Figaro.load

class My < ActiveRecord::Base
  self.abstract_class = true
  establish_connection(
    :adapter  => 'mysql',
    :database => ENV['mysql_database'],
    :host   => ENV['mysql_host'],
    :username => ENV['mysql_username'],
    :password => ENV['mysql_password'],
    :encoding => 'utf8',
    :collation => 'utf8_general_ci',
    :pool => 30
  )
end

class Feed < My; end
class Device < My; end

class RawStorer

  attr_accessor :sensors, :commands

  def bat i, v, t = true
    return i/10.0
  end

  def co i, v, t = true
    return i/1000.0
  end

  def light i, v, t = true
    return i/10.0
  end

  def nets i, v, t = true
    return i
  end

  def no2 i, v, t = true
    return i/1000.0
  end

  def noise i, v, t = true
    i = i/100.0
    if t
      return 0.0 if (i == 0)
      if v.to_s == "1.1"
        i = [0,i,110].sort[1]
        db = {0=>50,2=>55,3=>57,6=>58,20=>59,40=>60,60=>61,75=>62,115=>63,150=>64,180=>65,220=>66,260=>67,300=>68,375=>69,430=>70,500=>71,575=>72,660=>73,720=>74,820=>75,900=>76,975=>77,1050=>78,1125=>79,1200=>80,1275=>81,1320=>82,1375=>83,1400=>84,1430=>85,1450=>86,1480=>87,1500=>88,1525=>89,1540=>90,1560=>91,1580=>92,1600=>93,1620=>94,1640=>95,1660=>96,1680=>97,1690=>98,1700=>99,1710=>100,1720=>101,1745=>102,1770=>103,1785=>104,1800=>105,1815=>106,1830=>107,1845=>108,1860=>109,1875=>110}
      elsif v.to_s == "1.0"
        i = [0,i,103].sort[1]
        db = {0=>0,5 => 45,10 => 55,15 => 63,20 => 65,30 => 67,40 => 69,50 => 70,60 => 71,80 => 72,90 => 73,100 => 74,130 => 75,160 => 76,190 => 77,220 => 78,260 => 79,300 => 80,350 => 81,410 => 82,450 => 83,550 => 84,600 => 85,650 => 86,750 => 87,850 => 88,950 => 89,1100 => 90,1250 => 91,1375 => 92,1500 => 93,1650 => 94,1800 => 95,1900 => 96,2000 => 97,2125 => 98,2250 => 99,2300 => 100,2400 => 101,2525 => 102,2650 => 103}
      end
      return Mathematician.reverse_table_calibration(db, i)
    else
      return i
    end
  end

  def panel i, v, t = true
    return i/1000.0
  end

  def hum i, v, t = true
    # return round(7 + 125.0 / 65536.0  * $rawHum, 1);
    i = i/10.0
    if v.to_s == "1.1" and t
      i = (i - 7.0) / (125.0 / 65536.0)
    end
    return i
  end

  def temp i, v, t = true
    # round(-53 + 175.72 / 65536.0 * $rawTemp, 1);
    i = i/10.0
    if v.to_s == "1.1" and t
      i = (i + 53.0) / (175.72 / 65536.0)
    end
    return i
  end

  # code

  def initialize device
    keys = %w(temp bat co hum light nets no2 noise panel)
    batch_size = (ENV['batch_size'] || 8000).to_i
    count = Feed.where(device_id: device.id).count
    count = count/batch_size
    count += 1 if count%batch_size > 0
    total = count
    last_id = 0
    self.commands = []
    while count >= 0
      print "#{device.id} - #{count}/#{total}\n"
      ids = Feed.where("device_id = ? and id > ?", device.id, last_id).limit(batch_size).ids
      Feed.find(ids).each do |feed|
        begin
          ts = feed.timestamp.to_i * 1000
          keys.each do |sensor|
            metric = sensor
            value = feed[sensor]
            value = method(sensor).call( (Float(value) rescue value), device.kit_version)
            self.commands << "put #{metric} #{ts} #{value.round(4)} device_id=#{device.id} identifier=sck#{device.kit_version}"
          end
        rescue Exception => e
          raise "FAIL"
        end
      end
      last_id = ids.last
      count -= 1
    end
  end

end

Parallel.each(Device.order(id: :desc)) do |device|
  begin
    File.open("imports/#{device.id}.txt", 'w') do |file|
      file.write RawStorer.new(device).commands.join("\n")
      file.write "\n"
    end
    print [device.id,"OK\n".green].join("\t")
  rescue Exception => e
    print [device.id,"ERROR".red,"#{e.message.strip}\n"].join("\t")
  end
end
