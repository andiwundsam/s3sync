# This software code is made available "AS IS" without warranties of any        
# kind.  You may copy, display, modify and redistribute the software            
# code either by itself or as incorporated into your code; provided that        
# you do not remove any proprietary notices.  Your use of this software         
# code is at your own risk and you waive any claim against the author
# with respect to your use of this software code. 
# (c) 2007 s3sync.net
#

# The purpose of this file is to overlay the net/http library 
# to add some functionality
# (without changing the file itself or requiring a specific version)
# It still isn't perfectly robust, i.e. if radical changes are made
# to the underlying lib this stuff will need updating.

require 'net/http'

module Net

	$HTTPStreamingDebug = false

	# Allow request body to be an IO stream
	# Allow an IO stream argument to stream the response body out
	class HTTP
		alias _HTTPStreaming_request request
		
		def request(req, body = nil, streamResponseBodyTo = nil, &block)
			if not block_given? and streamResponseBodyTo and streamResponseBodyTo.respond_to?(:write)
				$stderr.puts "Response using streaming" if $HTTPStreamingDebug
				# this might be a retry, we should make sure the stream is at its beginning
				streamResponseBodyTo.rewind if streamResponseBodyTo.respond_to?(:rewind) and streamResponseBodyTo != $stdout 
				block = proc do |res|
					res.read_body do |chunk|
						streamResponseBodyTo.write(chunk)
					end
				end
			end
			if body != nil && body.respond_to?(:read)
				$stderr.puts "Request using streaming" if $HTTPStreamingDebug
				# this might be a retry, we should make sure the stream is at its beginning
				body.rewind if body.respond_to?(:rewind) 
				req.body_stream = body
				return _HTTPStreaming_request(req, nil, &block)
			else
				return _HTTPStreaming_request(req, body, &block)
			end
		end
	end

end #module

module S3sync
    class ProgressStream < SimpleDelegator
        def initialize(s, size=0)
            @start = @last = Time.new
            @total = size
            @transferred = 0
            @closed = false
            @printed = false
            @innerStream = s
            super(@innerStream)
            __setobj__(@innerStream)
        end
        # need to catch reads and writes so we can count what's being transferred
        def read(i)
            res = @innerStream.read(i)
            @transferred += res.respond_to?(:length) ? res.length : 0
            now = Time.new
            if(now - @last > 1) # don't do this oftener than once per second
                @printed = true
                begin
                    $stdout.printf("\rProgress: %db  %db/s  %s       ", @transferred, (@transferred/(now - @start)).floor, 
                                   @total > 0? (100 * @transferred/@total).floor.to_s + "%" : ""  
                                  )
                rescue FloatDomainError
                    #wtf?
                end
                $stdout.flush
                @last = now
            end
            res
        end
        def write(s)
            @transferred += s.length
            res = @innerStream.write(s)
            now = Time.new
            if(now -@last > 1) # don't do this oftener than once per second
                @printed = true
                $stdout.printf("\rProgress: %db  %db/s  %s       ", @transferred, (@transferred/(now - @start)).floor, 
                               @total > 0? (100 * @transferred/@total).floor.to_s + "%" : ""  
                              )  
                              $stdout.flush
                              @last = now
            end
            res
        end
        def rewind()
            @transferred = 0
            @innerStream.rewind if @innerStream.respond_to?(:rewind)
        end
        def close()
            $stdout.printf("\n") if @printed and not @closed
            @closed = true
            @innerStream.close
        end
    end

    class LimitStream < SimpleDelegator
        def initialize(s, bwlimit=0)
            @start = @last = Time.new

            @closed = false
            @innerStream = s
            super(@innerStream)
            __setobj__(@innerStream)

            @transfer_max = bwlimit * 128;
            @transfer_max = 512 if (@transfer_max < 512)
            @chunk_start = Time.now()
            @chunk_bytes = 0
            @limit_rate = bwlimit
            @sleep_adjust = 0.0
        end

        def sleep_for_bwlimit(bytes)
            now = Time.now()
            delta_t = now - @chunk_start
            @chunk_bytes += bytes

            # Calculate the amount of time we expect downloading the chunk
            # should take.  If in reality it took less time, sleep to
            # compensate for the difference.
            expected = @chunk_bytes.to_f / @limit_rate
            if expected > delta_t
                to_sleep = expected - delta_t  + @sleep_adjust
                if to_sleep < 0.2
                    # defer short sleep till next time
                    return
                end
                # adjust for scheduling
                t0 = Time.now()
                sleep to_sleep
                t1 = Time.now()
                @sleep_adjust = to_sleep - (t1 - t0)
                sleep_adjust = 0.5 if @sleep_adjust > 0.5
                sleep_adjust = -0.5 if @sleep_adjust < -0.5
            end
            @chunk_bytes = 0
            @chunk_start = Time.now()
        end

        # need to catch reads and writes so we can count what's being transferred
        def read(i)
            to_do = [i, @transfer_max].min
            res = @innerStream.read(to_do)
            @transferred += res.respond_to?(:length) ? res.length : 0
            sleep_for_bwlimit(bytes)
            res
        end

        def write(s)
            @transferred += s.length
            res = @innerStream.write(s)
            now = Time.new
            if(now -@last > 1) # don't do this oftener than once per second
                @printed = true
                $stdout.printf("\rProgress: %db  %db/s  %s       ", @transferred, (@transferred/(now - @start)).floor, 
                               @total > 0? (100 * @transferred/@total).floor.to_s + "%" : ""  
                              )  
                              $stdout.flush
                              @last = now
            end
            res
        end
        def rewind()
            @transferred = 0
            @innerStream.rewind if @innerStream.respond_to?(:rewind)
        end
        def close()
            $stdout.printf("\n") if @printed and not @closed
            @closed = true
            @innerStream.close
        end
    end
end #module
