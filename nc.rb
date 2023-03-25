require 'rubygems'
require 'bundler'
Bundler.require
Figaro.application = Figaro::Application.new(environment: "production", path: "application.yml")
Figaro.load

Dir['imports/*.txt'].sort.each do |file|
	puts file
	`cat #{file} | nc #{ENV['telnet_ip']} #{ENV['telnet_port']}`
	sleep(0.5) 
end
