Dir['tailimports/*.txt'].each do |file|
	`tail -n 18 #{file} > #{file}.tailed ; mv #{file}.tailed #{file}`
end
