module Mathematician

  def self.reverse_table_calibration( arr, raw_value )
    raw_value = raw_value.to_f
    arr = Hash[arr.collect{|k,v| [v,k] }].to_a.sort!

    if (raw_value >= arr.last[0])
      return arr.last[1].to_f
    end

    clamped_value = [arr.first[0], raw_value, arr.last[0]-1].sort[1]

    for i in (0..arr.length-1)
      if clamped_value >= arr[i][0] && clamped_value < arr[i+1][0]
        low, high = [arr[i], arr[i+1]]
        return self.linear_regression(clamped_value,low[1],high[1],arr[i][0],high[0])
      end
    end
  end

  def self.linear_regression( valueInput, prevValueOutput, nextValueOutput, prevValueRef, nextValueRef )
    slope = ( nextValueOutput.to_f - prevValueOutput.to_f ) / ( nextValueRef.to_f - prevValueRef.to_f )
    result = slope.to_f * ( valueInput.to_f - prevValueRef.to_f ) + prevValueOutput.to_f
    return result
  end

end
