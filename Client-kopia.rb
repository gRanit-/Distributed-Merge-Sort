require 'bunny'

def readArrayFromFile(fileName)
       begin
       file_contents = File.read(fileName)
       array = file_contents.scan(/\d+/)
       array.collect! &:to_i
       rescue Exception=>e
         puts "Couldn't read from file:"
                    print "=> "
                    print e
                    print " <=\n"
       end
       ensure
            return array
end

channel=nil
conn=nil
replayToQueue=nil
mergeQueue=nil
newTaskExchange=nil
finalCount=""
mergeQueueName=(0...50).map { ('a'..'z').to_a[rand(26)] }.join
replayToQueueName=(0...50).map { ('a'..'z').to_a[rand(26)] }.join
while true do 
    begin
        conn = Bunny.new
        conn.start
        channel = conn.create_channel
        #channel2 = conn.create_channel
        #newTaskExchange    = channel.fanout("NewTask")
        newTaskQueue=channel.queue("NewTaskQueue",:durable => true, :auto_delete => true)
        mergeQueue=channel.queue(mergeQueueName,:durable => true)
        replyToQueue=channel.queue(replayToQueueName,:durable => true, :auto_delete => true)
        #q2=channel.queue("SortQueue",:durable => true, :auto_delete => false)
        puts "Succesfully Connected to server!"
        break
        rescue Exception=>e
            puts "Something went terribly wrong! I couldn't connect to server the exception is:"
            print "=> "
            print e
            print " <=\n"
            for i in 0..5 do
                print "Recconecting in "
                print (5-i).to_s
                print  " seconds...\n"
                sleep(1)
            end    
        end
end

choice=0
numbers=Array.new

while choice!=1 and choice!=2 do
    puts "\n---------------------------------------------------------------------------------"
    puts "Press 1) Use file as an input"
    puts "Press 2) Generate 10000 random numbers between 0 and 1000000 and write them to file"
    puts "---------------------------------------------------------------------------------"
    puts "\nPlease choose 1 or 2 \n"

    choice=gets.chomp
    if choice=='1'
        puts "Enter FileName: "
        fileName=gets.chomp
        numbers=readArrayFromFile(fileName)
        if numbers.is_a?(Array)
            break
        else next
        end
    elsif choice=='2'

        for i in 0..9999 do
            random_number=Random.rand(1000000)
            numbers.push(random_number)
            File.open("Generated_Numbers.txt",'a'){|file| file.write(random_number)}
            if i<9999
                File.open("Generated_Numbers.txt",'a'){|file| file.write(" ")}
            end    
         end
         puts "Generated 1000 random numbers and saved it in Generated_Numbers.txt!"
        
        newTaskQueue.publish("NewTask",:persistent=>true,
            :headers=>{
                :taskID=>mergeQueue.name,
                :replyTo=>replyToQueue.name,
                :finalCount=>numbers.count
                  
            },
            :message_id=>mergeQueue.name 
            )
        puts "Send taskID: "+mergeQueue.name
        
         array=numbers
         finalCount=numbers.count
        while not array.empty?    do
            sliced=[]
            sliced<<array.shift
            sliced<<array.shift
            
            mergeQueue.publish("MergeMessage",:persistent=>true,
            :headers=>{
                    :taskID=>mergeQueue.name,
                    :replayTo=>replyToQueue.name,
                    :array=>sliced,
                    :finalCount=>numbers.count    
            }
            
        )
        
        end
        puts "Awaiting reply"   
        
         left=[]
         right=[]
        
        replyToQueue.subscribe(:block => false,:exclusive => true,:ack=>true) do |delivery_info, properties, payload|
               if left.empty?
                   left=properties.headers["array"]
                       puts "LEFT "+left.length.to_s+"\n"+left.inspect
                   else
                       right=properties.headers["array"]
                            
                    puts "RIGHT "+right.length.to_s+"\n"+right.inspect
                end
            
            if not left.empty?
                if left.length==finalCount
                    puts "FINISHED"
                    channel.acknowledge(delivery_info.delivery_tag, false)
                end
                end 
            
            if not right.empty?
                            if right.length==finalCount
                                puts "FINISHED"
                                replyQueue.purge()
                                mergeQueue.purge()
                                channel.acknowledge(delivery_info.delivery_tag, false)
                            end
                            end            
                    
            if not left.empty? and not right.empty?
                array=left+right
                if array.length==finalCount
                    puts "FINISHED"
                    channel.acknowledge(delivery_info.delivery_tag, false)
                else
            
           
            
                mergeQueue.publish("MergeMessage",:persistent=>true,
                        :headers=>{
                                :taskID=>mergeQueue.name,
                                :replayTo=>replyToQueue.name,
                                :array=>array,
                                :finalCount=>properties.headers["finalCount"]    
                        }


                    )
    
                left=[]
                right=[]    
       end      
   end
end

end
end
#msg  = ARGV.empty? ? "Hello World!........." : ARGV.join(" ")
#for number in numbers do

    
#end    
#puts " [x] Published #{msg}"
#puts " [x] Sent Hello World!"
#conn.close

