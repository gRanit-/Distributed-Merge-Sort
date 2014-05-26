require "bunny"
require "timeout"
require "SecureRandom"


class Worker

        attr_accessor :currentArrayLength, :currentTask, :queue, :id, :arraysNumber,:messagesSent

        def initialize(channel,id)
            @currentArrayLength=1
            @arraysNumber=0
            @currentTask=0
            @id=id
            @messagesSent=0
            @queue=channel.queue(@id,:durable => true, :auto_delete => true)
            puts "      Worker: created workerQueue for communication with id: "+@id

        end

end

class TaskManager

                
    attr_accessor :finishedMergeQueue,:freeWorkers,:inputMessages,:channel,:channel2,:conn,:clientQueue,:taskInputQueue,:newTaskQueue ,:newWorkerQueue,:finalCount,:mergeQueueName,:replayToQueueName,:workerResponseQueue,:workerList,:currentTaskQueue,:taskID
        
        #:taskExchange

    def initialize
        while true
            begin
                @conn = Bunny.new
                @conn.start
                @channel = @conn.create_channel
                @channel2= @conn.create_channel
                @finalCount=0
                @inputMessages=0
                @workerList0=[]
                @workerList1=[]
                @workerBussyList=[]
                @freeWorkers=0
                @finishedMergeQueue
                #@channel2.prefetch(1)
        #@channel2.prefetch(1)
        #@taskExchange=@channel.fanout("TaskExchange")
                @newTaskQueue=@channel2.queue("NewTaskQueue",:durable => true)
                @newWorkerQueue=@channel.queue("NewWorkerQueue",:durable => true, :auto_delete => true)
                puts @newWorkerQueue.name
                puts "Connected!"
                break
            rescue Exception=>e
                puts e
                sleep(2)
            end
        end        

    end

        
    def sendArray(array)
        while true
            workerList=[]
            workerList=@workerList1.select{|worker| worker.currentArrayLength==array.length}
            workerList.sort!{|x,y| x.messagesSent<=>y.messagesSent}
            worker=workerList[0]
            if not worker.nil?
                @workerList1.delete(worker)
            else
                

                workerList=@workerList0
                workerList.sort!{|x,y| x.messagesSent<=>y.messagesSent}
                worker=workerList[0]
                if not worker.nil?
                    @workerList0.delete(worker)
                else
                    workerList1=@workerList1.select{|worker| worker.currentArrayLength==array.length}
                    worker=workerList1[0]

                    puts workerList.inspect
                    puts "List0 "+@workerList0.length.to_s
                    puts "List1 "+@workerList1.length.to_s
                    puts "List2 "+@workerBussyList.length.to_s
                    if worker.nil?
                        return false
                    end    
                end
            end



            puts worker.arraysNumber
            
            worker.currentTask=@currentTaskQueue.name
            worker.queue.publish("ArrayToMerge",:persistent=>true,
                :headers=>{
                    :taskID=>@taskID,
                    :finalCount=>@finalCount,
                    :array=>array,
                    :taskID=>@taskID,:finishedQueue=>@finishedMergeQueue.name,:reject=>false},
                    :reply_to=>@taskInputQueue.name
                )
           
            worker.messagesSent=worker.messagesSent+1
            worker.arraysNumber=worker.arraysNumber+1
            worker.currentArrayLength=array.length

            if worker.arraysNumber==1
                @workerList1.push(worker)
            else @workerBussyList.push(worker)
                
            end    
                   @freeWorkers-=1
            
            
            puts "Sent to worker: "+worker.id
            return true
        end        
    end

    def start
        puts "TaskManager started successfuly"
        task_delivery_info, task_properties, task_payload=""
        while true
            
            while true
                msg=nil
                i=@newWorkerQueue.message_count
                while i!=0
                    delivery_info, properties, payload=@newWorkerQueue.pop
                    #headers=properties.header
                    puts "Got message: "+payload
                    puts properties[:headers]["workerID"]

                    worker=Worker.new(@channel,properties[:headers]["workerID"])
                    @workerList0.push(worker)
                    puts "New worker registered."
                    i=@newWorkerQueue.message_count
                    
                end


            #@newTaskQueue.subscribe(:ack=>true) do |task_delivery_info, task_properties, task_payload|
                msg=nil
                if @newTaskQueue.message_count!=0
                    msg=@newTaskQueue.pop()
                end
                if not msg.nil?
                    msg.compact!
                else
                    msg=[]    
                end
                if not msg.empty?

                   task_delivery_info, task_properties, task_payload=msg
                   puts task_payload
                   @inputMessages=0
                   @taskID=task_properties[:headers]["taskID"]
                   @finalCount=task_properties[:headers]["finalCount"]
                   puts "Got new task "+@taskID
                   
                   @currentTaskQueue=@channel.queue(SecureRandom.hex,:durable => true, :auto_delete => true)
                   @taskInputQueue=@channel2.queue(@taskID,:durable => true, :auto_delete => true)
                   @clientQueue=@channel.queue(task_properties[:reply_to],:durable => true, :auto_delete => true)
                   @finishedMergeQueue=@channel.queue(SecureRandom.hex,:durable => true, :auto_delete => true)
                   #@taskInputQueue.purge
                   #@currentTaskQueue.purge
                  break
                end
                
            end
        puts "Recieved task..."
        

        while true

            msg=nil
            i=@newWorkerQueue.message_count
            while i!=0
                delivery_info, properties, payload=@newWorkerQueue.pop
                #headers=properties.header
                puts "Got message: "+payload
                puts properties[:headers]["workerID"]

                worker=Worker.new(@channel,properties[:headers]["workerID"])
                @workerList0.push(worker)
                puts "New worker registered."
                i=@newWorkerQueue.message_count
                
            end


                msg=nil

            while @finishedMergeQueue.message_count!=0          #Worker informuje o skonczonej pracy
                    msg=@finishedMergeQueue.pop()
                    
                if not msg.nil?
                    msg.compact!
                    else
                      msg=[]  
                end
                if not msg.empty?

                    delivery_info,properties,payload=msg
                    puts payload
                    workerID=properties[:headers]["workerID"]

                    puts "Worker: "+workerID+" finished merging..."
                    worker=""

                    for worker in @workerList1
                        if worker.id==workerID
                            worker.currentArrayLength=0
                            worker.arraysNumber=0
                            @workerList0.push(worker)
                            @workerList1.delete(worker)
                        end
                    end
                    for worker in @workerBussyList
                        if worker.id==workerID
                            worker.currentArrayLength=0
                            worker.arraysNumber=0
                            @workerList0.push(worker)
                            @workerBussyList.delete(worker) 
                        end
                    end     
                end
            end
   

            #@channel2=conn.create_channel
            #@taskInputQueue=@channel2.queue(@taskID,:durable => true, :auto_delete => true)
                msg=nil
                if not @taskInputQueue.nil?
                     msg=@taskInputQueue.pop
                    
                    if not msg.nil?
                        msg.compact!
                        else
                            msg=[]  
                    end
                    if not msg.empty?
                    
                        delivery_info, properties, payload=msg
                        puts "Receiveing input"
                        array=properties[:headers]["array"]
                        if array.length==@finalCount
                            @workerList1.compact!
                            @workerBussyList.compact!
                            for worker in @workerList1

                                @workerList0.push(worker)
                            end
                            @workerList1=[]
                             for worker in @workerBussyList
                                 @workerList0.push(worker)
                             end   
                             @workerBussyList=[]
                            @clientQueue.publish("Result",:persistent=>true,
                                    :headers=>{
                                        :taskID=>@taskID,
                                        :finalCount=>@finalCount,
                                        :array=>array,
                                        :taskID=>@taskID},
                                        )
                            break
                        end

                        puts "Trying to send array..."
                        if not sendArray(array)
                            sleep(1.0/8.0)
                            if not sendArray(array)
                                sleep(1.0/8.0)
                                if not sendArray(array)   
                            puts "Can't find worker"
                            if properties[:headers]["reRouted"]
                                puts "REJECT COMMAND"
                                for worker in @workerList1
                                    worker.queue.publish("REJECT ARRAY",:persistent=>true,
                                    :headers=>{
                                    :taskID=>@taskID,
                                    :finalCount=>@finalCount,
                                    :taskID=>@taskID,:finishedQueue=>@finishedMergeQueue.name,:reject=>true},
                                    :reply_to=>@taskInputQueue.name

                                    )
                                    @workerList1.delete(worker)
                                    worker.currentArrayLength=0
                                    worker.arraysNumber=0
                                    @workerList0.push(worker)
                                end
                            end
                            puts "Rerouting message"
                            @taskInputQueue.publish("Re-routed array",:persistent=>true,
                                                        :headers=>{
                                                        :array=>array,
                                                        :finalCount=>properties[:headers]["finalCount"],
                                                        :workerID=>0,
                                                        :reRouted=>true}

                                )
                        end
                        
                    end
                end
                    end
                end
            end    
                 #@channel2.acknowledge(task_delivery_info.delivery_tag,false)   
                    
                #end
        end
    end
end



taskManager=TaskManager.new()
taskManager.start    
