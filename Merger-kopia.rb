require "bunny"
require "SecureRandom"

class Merger
    attr_accessor :conn,
    :currentTaskQueue,
    :replyToQueue,
    :taskQueue,
    :channel,
    :channel2,
    :channel3,
    :newTaskExchane,
    :taskID,
    :finalCount,
    :workerID,
    :task_delivery_info,
    :task_properties,
    :task_payload
    :workerQueue

    def initialize()
        @conn = Bunny.new
        @conn.start    
        @channel  = @conn.create_channel
        @channel2  = @conn.create_channel
        @channel3  = @conn.create_channel
        @channel4  = @conn.create_channel
        @newTaskExchange=@channel.fanout("NewTask")

        
        @channel.prefetch(1)
        @channel3.prefetch(2)
        @taskQueue=@channel.queue("",:durable => true, :auto_delete => true,:exclusive => true)

        @taskID=""
        @finalCount=""
        @workerID=SecureRandom.hex
    
        @workerQueue=@channel3.queue(@workerID,:durable => true, :auto_delete => true)
        
        
        newWorker=@channel.queue("NewWorkerQueue",:durable => true, :auto_delete => true)
        newWorker.publish("Requesting signup, id: "+@customerID,:persistent=>true,:headers=>{
        :workerID=>@workerID})
        
        noMessage=true

        puts "Awaiting Task..."

        
        @replyToQueue=@channel2.queue(customerID,:durable => true, :auto_delete => true)
            puts "Connected to replyQueue: "+customerID 
        @currentTaskQueue=@channel3.queue("CurrentTaskQueue",:durable => true)
            puts "Connected to CurrentTaskQueue: "+taskID  
        # currentTaskQueue=@currentTaskQueue
            self.merge()
            puts "Finished Task!"       
    end

   
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
            @currentTaskQueue=@channel3.queue(taskID,:durable => true)
            puts "Connected to currentTaskQueue: "+taskID  

         
            
            #currentTaskQueue=@currentTaskQueue
            self.merge()
            puts "Finished Task!"  
        end
        puts "Out ouf taskQueue"
    end


    def mergeArrays(left, right)
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

        @workerQueue.subscribe(:ack=>true) do |delivery_info, properties, payload|
            puts "Recieved array to sort" 
            array=properties.headers["array"]
            if left.empty?
                left=array
            else
                right=array
            end
            if not left.empty? and not right.empty?
                array=mergeArrays(left,right)    
                @currentTaskQueue.publish("NewArray",:headers=>{
                    :array=>array,
                    :finalCount=>properties.headers["finalCount"]
                })
                left=[]
                right=[]
                channel3.acknowledge(delivery_info.delivery_tag, false) 
            end     
            
        end

    end  
end
   
m=Merger.new

puts "Starting..."
m.start
