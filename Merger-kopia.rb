require "bunny"


class Merger
    attr_accessor :conn,
    :mergeQueue,
    :replyToQueue,
    :taskQueue,
    :channel,
    :channel2,
    :channel3,
    :newTaskExchane,
    :taskID,
    :finalCount,
    :customerID,
    :task_delivery_info,
    :task_properties,
    :task_payload


    def initialize()
        @conn = Bunny.new
        @conn.start    
        @channel  = @conn.create_channel
        @channel2  = @conn.create_channel
        @channel3  = @conn.create_channel
        @channel4  = @conn.create_channel
        @newTaskExchange=@channel.fanout("NewTask")

        
        @channel.prefetch(1)
        #@channel3.prefetch(2)
        @taskQueue=@channel.queue("",:durable => true, :auto_delete => true,:exclusive => true)
        @taskQueue.bind(@newTaskExchange)
        @taskID=""
        @finalCount=""
        @customerID=(0...50).map { ('a'..'z').to_a[rand(26)] }.join
    
        q=@channel.queue(@customerID,:durable => true, :auto_delete => true)
        
        
        newWorker=@channel.queue("NewWorkerQueue",:durable => true, :auto_delete => true)
        newWorker.publish("Requesting signup, id: "+@customerID,:persistent=>true,:headers=>{
        :workerID=>@customerID})
        
        noMessage=true
        puts "Awaiting Task"
        while noMessage do
            q.subscribe() do |task_delivery_info, task_properties, task_payload|
                @customerID=task_properties.headers["replyTo"]
                @taskID=task_properties.headers["taskID"]
                @finalCount=task_properties.headers["finalCount"]
                noMessage=false
            end
        end
        
        @replyToQueue=@channel2.queue(customerID,:durable => true, :auto_delete => true)
            puts "Connected to replyQueue: "+customerID 
        @mergeQueue=@channel3.queue(taskID,:durable => true)
            puts "Connected to mergeQueue: "+taskID  
        # mergeQueue=@mergeQueue
            self.merge()
            puts "Finished Task!"       
    end

    def getRandomString
        return (0...50).map { ('a'..'z').to_a[rand(26)] }.join
    end


    def start
        @taskQueue.subscribe(:block => true,:ack=>false,:exclusive => true) do |task_delivery_info, task_properties, task_payload|
            
            @task_delivery_info, @task_properties, @task_payload=task_delivery_info, task_properties, task_payload
            puts task_payload
            @customerID=task_properties.reply_to
            @taskID=task_properties.headers["taskID"]
            @finalCount=task_properties.headers["finalCount"]
            puts taskID

            @channel2  = @conn.create_channel
            @channel3  = @conn.create_channel
            #@channel3.prefetch(1)
            @replyToQueue=@channel2.queue(customerID,:durable => true, :auto_delete => true)
            puts "Connected to replyQueue: "+customerID 
            @mergeQueue=@channel3.queue(taskID,:durable => true)
            puts "Connected to mergeQueue: "+taskID  

         
            
            #mergeQueue=@mergeQueue
            self.merge()
            puts "Finished Task!"  
        end
        puts "Out ouf taskQueue"
    end


    def mergeSort(left, right)
      result = []
      until left.empty? || right.empty?
        if left.first <= right.first
          result << left.shift
        else
          result << right.shift
        end
      end
      return result + left + right
    end    



    def merge()
        left=[]
        right=[]
    #while true do
     #   finish=false
       
        @mergeQueue.subscribe(:ack=>true) do |delivery_info, properties, payload|
            puts "Recieved array to sort" 
            array=properties.headers["array"]
            array.insertionsort
            @replyToQueue.publish("ArrayToMerge",:headers=>{
                :array=>array,
                :finalCount=>properties.headers["finalCount"]
            })
           channel3.acknowledge(delivery_info.delivery_tag, false)  
            
    end

    #break if finish
#end

end  
 end
   
m=Merger.new

puts "Starting..."
m.start
'''
while true do
delivery_info1=" "
delivery_tag1=" "
delivery_info2=" "
delivery_tag2=" "
begin
st=" "
   delivery_info1, properties1, payload1, = mergeQueue.pop

    if payload1!=nil 
        left=payload1.split(" ").map {|i| i.to_i}
        i+=1

       delivery_info2, properties2, payload2 = mergeQueue.pop

        if payload2!=nil 
            right=payload2.split(" ").map {|i| i.to_i}
 
        else right=[]
        end  
            array=merge(left,right)

            for number in array do
                st+=number.to_s+" "
            end 

            puts st+"\n"+array.length.to_s+"\n"
        #    
        mergeQueue.publish(st,:persistent=>true)

        #end
    end

    rescue Exception => e
#print st
      print e
      wait(2)
      channel.reject(delivery_info1.delivery_tag, true)
      channel.reject(delivery_info2.delivery_tag, true)
      channel.close
      conn.close
      break
 
    end

end
'''