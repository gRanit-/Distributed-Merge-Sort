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
    :task_payload,
    :workerQueue,
    :finishedTaskQueue

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
        #@taskQueue=@channel.queue("",:durable => true, :auto_delete => true,:exclusive => true)

        @taskID=""
        @finalCount=""
        @workerID=SecureRandom.hex
    
        @workerQueue=@channel3.queue(@workerID,:durable => true, :auto_delete => true)
        
        
        newWorker=@channel.queue("NewWorkerQueue",:durable => true, :auto_delete => true)
        newWorker.publish("Requesting signup, id: "+@workerID,:persistent=>true,:headers=>{
        :workerID=>@workerID,
        :test=>"test"})
        
        noMessage=true

        puts "Awaiting Task..."

        
        #@replyToQueue=@channel2.queue(worker,:durable => true, :auto_delete => true)
        #    puts "Connected to replyQueue: "+customerID 
        #@currentTaskQueue=@channel3.queue("CurrentTaskQueue",:durable => true, :auto_delete => true)
        #    puts "Connected to CurrentTaskQueue: "+taskID  
        # currentTaskQueue=@currentTaskQueue
            #self.merge()

    end

   
    


    def start
            
        while true
        self.merge()
        end
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
        while true

        
        while @workerQueue.message_count!=0
            delivery_info, properties, payload=@workerQueue.pop
            if properties[:headers]["reject"]
                sleep(Random.rand(3)/5.0)
                puts "REJECTING"
                puts left.length.to_s
                left.compact!

                @channel3.queue(properties[:reply_to],:durable => true, :auto_delete => true).publish("New Merged Array",:headers=>{
                    :array=>left,
                    :finalCount=>properties[:headers]["finalCount"],
                    :workerID=>@workerID
                })

                left=[]
                right=[]
            else

            array=properties[:headers]["array"]
            puts "Recieved array to sort " +array.length.to_s
            array.compact!
            puts array.inspect
            if left.empty?
                left=array
            else
                right=array
            end
            if not left.empty? and not right.empty?
                leftn=left.length
                rightn=right.length
                array=mergeArrays(left,right)
                array.compact!   
                puts "Merged left: "+leftn.to_s+" with right: "+rightn.to_s

                @channel3.queue(properties[:reply_to],:durable => true, :auto_delete => true).publish("New Merged Array",:headers=>{
                    :array=>array,
                    :finalCount=>properties[:headers]["finalCount"],
                    :workerID=>@workerID,
                    :reRouted=>false
                })
                @channel.queue(properties[:headers]["finishedQueue"],:durable => true, :auto_delete => true).publish("Finished",:headers=>{
                    :workerID=>@workerID
                })
                puts "Sent it to TaskManager..."

                left=[]
                right=[]
            elsif  left.empty? and  right.empty?
                                @channel.queue(properties[:headers]["finishedQueue"],:durable => true, :auto_delete => true).publish("Finished",:headers=>{
                    :workerID=>@workerID
                })
            end     
            end
        end
    end

    end  
end
   
m=Merger.new

puts "Starting..."
m.start
