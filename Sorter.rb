require "bunny"

conn = Bunny.new
conn.start
channel=conn.create_channel
q=channel.queue("MergeQueue",:durable => true, :auto_delete => false)


class Array
  def insertionsort!
    1.upto(length - 1) do |i|
      value = self[i]
      j = i - 1
      while j >= 0 and self[j] > value
        self[j+1] = self[j]
        j -= 1
      end
      self[j+1] = value
    end
    self
  end
end

puts " [*] Waiting for messages in #{q.name}. To exit press CTRL+C"
q.subscribe(:ack => true, :block => true) do |delivery_info, properties, body|
  puts " [x] Received #{body}"
    puts properties.headers["array"].inspect
  # imitate some work
  #sleep body.count(".").to_i
  puts " [x] Done"

  channel.ack(delivery_info.delivery_tag)
end