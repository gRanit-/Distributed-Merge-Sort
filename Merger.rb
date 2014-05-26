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

        
        while @workerQueue.message_count!=0     #Pobieranie tablic
            delivery_info, properties, payload=@workerQueue.pop(:ack=>true)
            if properties[:headers]["reject"]       #Komenda odrzucenia tablicy i oddania na kolejke
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
             @channel3.acknowledge(delivery_info.delivery_tag,false)
            end

        end
    end

    end  
end
   
m=Merger.new

puts "Starting..."
m.start
