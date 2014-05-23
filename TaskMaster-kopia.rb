require "bunny"
require "SecureRandom"



class TaskManager

    class Worker
        attr_accessor
            :currentArrayLength
            :currentTask
            :queue
            :id
            :arraysNumber
        def initialize(channel,id)
            @currentArrayLength=0
            @arraysOnQueue
            @currentTask=0
            @id=id
            @queue=channel.queue(@id,:durable => true, :auto_delete => true)

        end
    end
                
    attr_accessor
        :channel
        :channel2
        :conn
        :replayToQueue
        :mergeQueue
        :newTaskQueue
        :finalCount
        :mergeQueueName
        :replayToQueueName
        :workerResponseQueue
        :workerList
        :currentTaskQueue
        :taskID
        :taskInputQueues
        #:taskExchange

    def initialize
        @conn = Bunny.new
        @conn.start
        @channel = @conn.create_channel
        @channel2= @conn.create_channel
        @channel2.prefetch(1)
        @finalCount=0
        @workerList0=[]
        @workerList1=[]
        @workerBussyList=[]

        @channel2.prefetch(1)
        #@taskExchange=@channel.fanout("TaskExchange")
        @newTaskQueue=@channel2.queue("NewTaskQueue",:durable => true)
        @newWorkerQueue=@channel.queue("NewWorkerQueue",:durable => true, :auto_delete => true)
        @workerResponseQueue=@channel.queue("workerResponseQueue",:durable => true, :auto_delete => true)
    end

    def binarySearch(value,workerList)

        low, high = 0, workerList.length - 1
        while low <= high
            mid = (low + high) / 2
            case
                when workerList[mid].currentArrayLength > value then high = mid - 1
                when workerList[mid].currentArrayLength < value then low = mid + 1
                else return mid
            end
        end
        nil
     end

        
    def sendArray(array)
        while true
            worker=binarySearch(worker.currentArrayLength,workerList1)
            workerList1.delete(worker)
            if worker.nil?
                worker=binarySearch(worker.currentArrayLength,workerList0)
                workerList0.delete(worker)
            else wait(1)
            end


            worker.arraysNumber++
            if worker.arraysNumber==1
                workerList1.push(worker)
            elsif worker.arraysNumber==0
                workerList0.push(worker)
            else workerBussyList.push(worker)
            end    
                        
            
            worker.currentArrayLength=array.length
            worker.arraysNumber++
            worker.currentTask=@currentTaskQueue.name
            worker.queue.publish("ArrayToMerge",:persistent=>true,
                :headers=>{
                    :taskID=>@currentTaskQueue.name,
                    :finalCount=>@finalCount,
                    :array=>array  
                },
                :message_id=>@currentTaskQueue.name
                )

        end        
    end

    def start

        task_delivery_info, task_properties, task_payload=""
        @newTaskQueue.subscribe(:block=>true,:exclusive => true,:ack=>true) do |task_delivery_info, task_properties, task_payload|
            puts task_payload
            @taskID=task_properties.headers["taskID"]
            @finalCount=task_properties.headers["finalCount"]
            @currentTaskQueue=@channel.queue("CurrentTaskQueue",:durable => true, :auto_delete => true)
            @taskInputQueue=@channel.queue(@taskID,:durable => true, :auto_delete => true)
            @replayToQueue=@channel.queue(task_properties.reply_to,:durable => true, :auto_delete => true)
           

            @taskInputQueue.subscribe() do |delivery_info, properties, payload|
                sendArray(properties.headers["array"])

            end

 

        end
        
        @currentTaskQueue.subscribe(:block=>true) do |delivery_info, properties, payload|
            array=properties.headers["array"]
            if array.length==@finalCount
                @replayToQueue.publish("Result",:properties=>{:array=>array})
                @channel2.acknowledge(task_delivery_info.delivery_tag,false)
            else    
                sendArray(array)
            end
        end

        @newWorkerQueue.subscribe() do |delivery_info, properties, payload|
            puts "Got message: "+payload

            worker=Worker.new(@channel,properties.headers["workerID"])
            workerList0.push(worker)
            puts "New worker registered."
        end

    end
end

taskMaster=TaskManager.new
taskMaster.start    
