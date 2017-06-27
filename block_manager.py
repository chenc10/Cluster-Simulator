from cluster import Cluster
from event import (EventCacheHit, EventCacheMiss)
from rdd import RDD
import os
from machine import Machine


class BlockManager:
    #Memory_Disk_Ratio = 3  # ratio of the speed in reading from memory and disk
    def __init__(self, machines):
        self.machines = machines
        self.cache_map = dict()  # all the blocks that are cached in the cluster, map: (block_id, machine_id)
        # self.incomplete_rdd_ids = list()

        self.lru = False
        self.ref_count = False
        self.strict_sticky = False
        self.conservative_sticky = False
        self.lfu = False
        self.evict_dead = False
        self.sticky = False  # True or False


        self.rdd_list = list()

        cache_log_dir = os.getcwd()  ## get current working directory
        self.cache_log = open("%s/cache_log.txt" % cache_log_dir, "w")  # log for tracking the cache status
        self.access_pattern = open("%s/access_pattern.txt" % cache_log_dir, "a+")  # log for tracking the access pattern
        self.access_pattern.write("ref_count rank \t recency rank \t recency rank without dead block \t hist_freq_rank\n")
        self.ref_count_online_error_write = open("%s/ref_count_online_error_write.txt" % cache_log_dir, "a+")
        self.ref_count_online_error_write.write("Application starts\n")
        self.ref_count_online_error_read = open("%s/ref_count_online_error_read.txt" % cache_log_dir, "a+")  # log for tracking the online error of the ref count approximation
        self.ref_count_online_error_read.write("Application starts\n")

    def get_blocks(self, machine, blocks, time, to_produce_block):# get the block in any of the machine of the cluster
        hit_count = 0
        miss_count = 0
        for block in blocks:
            if not hasattr(block, 'id'):
                1;
            self.cache_log.write("%s:\tAccess %s\n" % (time, block.id))
            if block.id in self.cache_map.keys():
                hit_count += 1
                machine_caching_the_block = self.search_machine_by_id(self.cache_map[block.id])
                self.cache_log.write("\t\t Found %s in Machine %s\n" % (block.id, machine_caching_the_block.id))
                # for statisitc analysis (relationship of the probabilities of the next access and the remaining ref_count)
                size_with_greater_ref_count = 0.0 # including yourself
                number_with_greater_ref_count = 0.0  # not including yourself
                number_with_the_same_ref_count = 0.0
                number_with_greater_hist_freq = 0.0
                number_with_the_same_hist_freq = 0.0

                for cached_block in machine_caching_the_block.cached_blocks:
                    # if cached_block.ref_count >= block.ref_count:
                      #  size_with_greater_ref_count += cached_block.size
                    if cached_block.ref_count > block.ref_count:
                        number_with_greater_ref_count += 1
                    elif cached_block.ref_count == block.ref_count:
                        number_with_the_same_ref_count += 1
                    if cached_block.history_frequency > block.history_frequency:
                        number_with_greater_hist_freq += 1
                    elif cached_block.history_frequency == block.history_frequency:
                        number_with_the_same_hist_freq += 1

                number_with_the_same_ref_count -= 0  # ?deduct yourself
                number_with_the_same_hist_freq -= 1
                cached_blocks = list(machine_caching_the_block.cached_blocks)  # make a copy of the cached blocks
                cached_blocks.reverse()  # now in the descending order of recency
                size_with_greater_recency_not_dead = 0.0 # including yourself
                number_with_greater_recency_not_dead = 0.0 # not including yourself
                size_with_greater_recency = 0.0  # including yourself
                number_with_greater_recency = 0.0  # not including yourself
                for cached_block in cached_blocks:
                    if cached_block == block:
                        break
                    number_with_greater_recency += 1
                    if cached_block.ref_count != 0:
                        number_with_greater_recency_not_dead += 1

                ref_rank_ratio = (number_with_greater_ref_count + 0.5 * number_with_the_same_ref_count)/len(machine_caching_the_block.cached_blocks)
                ref_rank_online_error = block.ref_count - block.ref_count_by_job
                # # we take half of the number with the same ref count because it randomly chooses one among them
                # recency_rank_ratio = (size_with_greater_recency+0.0)/total_size
                recency_rank_ratio = number_with_greater_recency / len(machine_caching_the_block.cached_blocks)
                # recency_rank_without_dead_block_ratio = (size_with_greater_recency_not_dead+0.0)/total_size
                recency_rank_without_dead_block_ratio = number_with_greater_recency_not_dead / len(machine_caching_the_block.cached_blocks)

                hist_rank_ratio = (number_with_greater_hist_freq + 0.5 * number_with_the_same_hist_freq)/len(machine_caching_the_block.cached_blocks)

                self.access_pattern.write("%s\t%s\t%s\t%s\n" %
                    (ref_rank_ratio, recency_rank_ratio, recency_rank_without_dead_block_ratio, hist_rank_ratio))

                self.ref_count_online_error_read.write("%s\n" % ref_rank_online_error)


                if self.lru == True:  # update the access order
                    machine_caching_the_block.remove_block(block)
                    machine_caching_the_block.add_block(block)
                block.ref_count -= 1
                block.ref_count_by_job -= 1


            else: # when cache miss, will the missed block be re-admitted in the memory? currently it is yes
                self.cache_log.write("\t\t Miss %s in cache\n" % block.id)
                block.ref_count -= 1  # Although now it is a miss, the referenced count of this block should be deducted by 1
                block.ref_count_by_job -= 1
                miss_count += 1
                self.put_blocks(machine, [block], time)
                #  self.check_incomplete_rdd(block.rdd_id, block.user_id)  # when a block is re-admitted, check whether this rdd is complete
            if to_produce_block.is_complete:
                block.eff_ref_count_blk -= 1
            if to_produce_block.rdd.is_complete:
                block.eff_ref_count_rdd -= 1

            block.history_frequency += 1  # history access frequency.
            self.cache_log.write('( Block %s: ref count : %s, Eff blk ref count: %s, Eff RDD ref count: %s)\n' % (block.id, block.ref_count, block.eff_ref_count_blk, block.eff_ref_count_rdd))

        return [hit_count, miss_count]

    def put_blocks(self, machine, blocks, time):

         for block in blocks:
            ref_rank_online_error = block.ref_count - block.ref_count_by_job
            self.ref_count_online_error_write.write("%s\n" % ref_rank_online_error)

            if not block.has_been_generated:
                self.get_ref_count(block)
                block.has_been_generated = 1
            if machine.remaining_cache_size >= block.size:
                self.do_put(machine, block, time)
                #self.check_incomplete_rdd(block.rdd_id)
                #return True
            elif self.admission_check(machine, block, time): ## In RefCount, check whether this block should be cached.
                self.cache_log.write("%s:\tClear %s Bytes for %s in Machine %s\n" % (time, block.size, block.id, machine.id))
                self.make_room(machine, block.size - machine.remaining_cache_size, block.rdd_id, time)
                self.do_put(machine, block, time)


    def do_put(self, machine, block, time):

        machine.add_block(block) # machine.remaining_cache_size will be updated in this function
        self.cache_map[block.id] = machine.id

        ## do put a block. In LRC, this block must be newly generated??. GO and find its (effective) ref_counts
        ## It could also be the rdd that is rejected on admission check
        ## how to differitiate the above two cases? For the second case, the remaining reference count might be defferent : get the ref_count before admission check

        self.cache_log.write("%s:\tPut %s in Machine %s, RefCount: %s\n" % (time, block.id, machine.id, block.ref_count))

        self.cache_log.write('( Block %s: ref count : %s, Eff blk ref count: %s, Eff RDD ref count: %s)\n' % (block.id, block.ref_count, block.eff_ref_count_blk, block.eff_ref_count_rdd))

    def get_ref_count(self, block):

        ref_me_rdds = block.rdd.ref_me_rdds
        if block.is_shuffle:
            for ref_me_rdd_id in ref_me_rdds:
                ref_me_rdd = self.search_rdd_by_id(ref_me_rdd_id)
                if ref_me_rdd.is_complete:
                    block.eff_ref_count_rdd += ref_me_rdd.partitions
                    block.eff_ref_count_blk += ref_me_rdd.partitions
                else:
                    for ref_me_block in ref_me_rdd.blocks:
                        if ref_me_block.is_complete:
                            block.eff_ref_count_blk += 1
                block.ref_count += ref_me_rdd.partitions
        else:
            for ref_me_rdd_id in ref_me_rdds:
                ref_me_rdd = self.search_rdd_by_id(ref_me_rdd_id)
                if ref_me_rdd == False:
                    1
                if ref_me_rdd.is_complete:
                    block.eff_ref_count_rdd += 1
                if ref_me_rdd.blocks[block.index].is_complete:
                    block.eff_ref_count_blk += 1
                block.ref_count += 1


    def do_remove(self, machine, block, time):
        machine.remove_block(block)
        del self.cache_map[block.id]
        # do remove a block, blocks referencing it become incomplete. All its peers' effective ref count should be updated
        ref_me_rdds = block.rdd.ref_me_rdds # ids
        if block.is_shuffle: # every block of the ref_me_rdds becomes incomplete
            for ref_me_rdd_id in ref_me_rdds:
                ref_me_rdd = self.search_rdd_by_id(ref_me_rdd_id)
                for this_block in ref_me_rdd.blocks:
                    if not this_block.has_been_generated: # if this block has been generated, nothing happens
                        if this_block.is_complete: # if it is already incomplete, nothing happens
                            this_block.is_complete = 0
                            for to_notify_rdd_id in ref_me_rdd.to_ref_rdds:
                                to_notify_rdd = self.search_rdd_by_id(to_notify_rdd_id)
                                to_notify_rdd.ref_me_block_incomplete(this_block.index)
                            if ref_me_rdd.is_complete:
                                ref_me_rdd.is_complete = 0
                                for to_notify_rdd_id in ref_me_rdd.to_ref_rdds:
                                    to_notify_rdd = self.search_rdd_by_id(to_notify_rdd_id)
                                    to_notify_rdd.ref_me_rdd_incomplete(ref_me_rdd.partitions)
        else:
            index = block.index
            for ref_me_rdd_id in ref_me_rdds:
                ref_me_rdd = self.search_rdd_by_id(ref_me_rdd_id)
                this_block = ref_me_rdd.blocks[index]
                if not this_block.has_been_generated:
                    if this_block.is_complete:  # if it is already incomplete, nothing happens
                        this_block.is_complete = 0
                        for to_notify_rdd_id in ref_me_rdd.to_ref_rdds:
                            to_notify_rdd = self.search_rdd_by_id(to_notify_rdd_id)
                            to_notify_rdd.ref_me_block_incomplete(this_block.index)
                        if ref_me_rdd.is_complete:
                            ref_me_rdd.is_complete = 0
                            for to_notify_rdd_id in ref_me_rdd.to_ref_rdds:
                                to_notify_rdd = self.search_rdd_by_id(to_notify_rdd_id)
                                to_notify_rdd.ref_me_rdd_incomplete(ref_me_rdd.partitions)


    def admission_check(self, machine, block, time):
        decision = True
        total_size_of_other_rdds = 0 # we do not evit the blocks of the same rdd
        for cached_block in machine.cached_blocks:
            if cached_block.rdd_id != block.rdd_id:
                total_size_of_other_rdds += cached_block.size
        if block.size > total_size_of_other_rdds:
            self.cache_log.write("%s: \t Reject -- Larger than the total size of other RDDs: %s\n" % (time, block.id))
            decision =False
        elif self.ref_count == True:  ## find all the blocks that have smaller ref_count than this block. If their total size can't accommodate this block, reject.
            cleared_size = 0
            total_ref_of_cleared_blocks = 0
            for cached_block in machine.cached_blocks:
                if cached_block.rdd_id != block.rdd_id and cached_block.ref_count + total_ref_of_cleared_blocks <= block.ref_count:
                    cleared_size += cached_block.size
                    total_ref_of_cleared_blocks += cached_block.ref_count
            if cleared_size < block.size:  #+ machine.remaining_cache_size ?
                self.cache_log.write("%s: \t Reject -- Referenced Count: %s\n" % (time, block.id))
               # if block.rdd_id not in self.incomplete_rdd_ids:
                #    self.incomplete_rdd_ids.append(block.rdd_id)
                decision = False

        elif self.conservative_sticky == True:
            cleared_size = 0
            total_ref_of_cleared_blocks = 0
            for cached_block in machine.cached_blocks: #??? the first condition still needed?
                if cached_block.eff_ref_count_blk + total_ref_of_cleared_blocks <= block.eff_ref_count_blk:
                    cleared_size += cached_block.size
                    total_ref_of_cleared_blocks += cached_block.eff_ref_count_blk
            if cleared_size < block.size:  # + machine.remaining_cache_size ?
                self.cache_log.write("%s: \t Reject -- Referenced Count with Conserative Sticky: %s\n" % (time, block.id))
                decision = False

        elif self.strict_sticky == True:
            cleared_size = 0
            total_ref_of_cleared_blocks = 0
            for cached_block in machine.cached_blocks:  # ??? the first condition still needed?
                if cached_block.eff_ref_count_rdd + total_ref_of_cleared_blocks <= block.eff_ref_count_rdd:
                    cleared_size += cached_block.size
                    total_ref_of_cleared_blocks += cached_block.eff_ref_count_rdd
            if cleared_size < block.size:  # + machine.remaining_cache_size ?
                self.cache_log.write("%s: \t Reject -- Referenced Count with Strict Sticky: %s\n" % (time, block.id))
                decision = False

        return decision

    def make_room(self, machine, size, rdd_id, time): # don't remove the blocks from the same rdd: why? the default behavior of Spark
        cleared_size=0
        if self.sticky:
            ## evict those incomplete rdds first
            remove_list = list()  # do not delete the element of a list while iterating it
            for block in machine.cached_blocks:
                if block.rdd_id in self.incomplete_rdd_ids and block.rdd_id != rdd_id:
                    self.cache_log.write("\t\tEvict -- Sticky: %s\n" % block.id)
                    remove_list.append(block)
                    cleared_size += block.size
                if cleared_size >= size:
                    for block in remove_list:
                        self.do_remove(machine, block, time)
                    return True

            for block in remove_list:  # if it comes here, the blocks evicted by sticky policy are not enough for the target size.
                self.do_remove(machine, block, time)

        if self.lru:
            remove_list = list()
            removed_rdd_list = list() # record for sticky policy
            for block in machine.cached_blocks:
                if block.rdd_id != rdd_id and block.rdd_id not in removed_rdd_list: # will remove the blocks of this rdd all at once
                    if self.sticky: # newly generated incomplete RDD
                        blocks_of_the_rdd = machine.find_all_blocks_by_rdd_id(block.rdd_id)
                        for block_of_the_rdd in blocks_of_the_rdd:
                            remove_list.append(block_of_the_rdd)
                            cleared_size += block_of_the_rdd.size
                            if cleared_size >= size:
                                for block_to_evict in remove_list:
                                    self.cache_log.write("\t\tEvict -- LRU + Sticky: %s\n" % block_to_evict.id)
                                    self.do_remove(machine, block_to_evict, time)
                                return True
                        removed_rdd_list.append(block.rdd_id)
                    else:
                        remove_list.append(block)
                        cleared_size += block.size
                        if cleared_size >= size:
                            for block_to_evict in remove_list:
                                self.cache_log.write("\t\tEvict -- LRU: %s\n" % block_to_evict.id)
                                self.do_remove(machine, block_to_evict, time)
                            return True

        elif self.ref_count:
            cached_blocks = machine.cached_blocks   #cached_block_id
            ref_map =dict() # (block_id, ref_count)
            for block in cached_blocks:  # will the recency be kept? seems not
                ref_map[block.id] = block.ref_count
            sorted_map = sorted(ref_map.items(), key=lambda x: x[1])  ## sort the ref_count from the smallest to the largest
            remove_list = list()
            removed_rdd_list = list()  # record for sticky policy ? why no need anymore
            for block in sorted_map: # block_id, ref_count
                block_to_evict = machine.search_block_by_id(block[0])
                if block_to_evict.rdd_id != rdd_id and block_to_evict.rdd_id not in removed_rdd_list:
                    if self.sticky:
                        blocks_of_the_rdd = machine.find_all_blocks_by_rdd_id(block_to_evict.rdd_id)
                        for block_of_the_rdd in blocks_of_the_rdd:
                            remove_list.append(block_of_the_rdd)
                            cleared_size += block_of_the_rdd.size
                            if cleared_size >= size:
                                for block_to_evict in remove_list:
                                    self.cache_log.write("\t\tEvict -- Referenced Count + Sticky: %s\n" % block_to_evict.id)
                                    self.do_remove(machine, block_to_evict, time)
                                return True
                        removed_rdd_list.append(block_to_evict.rdd_id)
                    else:
                        if block_to_evict.rdd_id != rdd_id:
                            self.cache_log.write("\t\tEvict -- Referenced Count: %s\n" % block_to_evict.id)
                            remove_list.append(block_to_evict)
                            cleared_size += block_to_evict.size
                            if cleared_size >= size:
                                for block_to_evict in remove_list:
                                    self.do_remove(machine, block_to_evict, time)
                                return True
        elif self.conservative_sticky:
            cached_blocks = machine.cached_blocks  # cached_block_id
            ref_map = dict()  # (block_id, ref_count)
            for block in cached_blocks:  # will the recency be kept? seems not
                ref_map[block.id] = block.eff_ref_count_blk
            sorted_map = sorted(ref_map.items(),
                                key=lambda x: x[1])  ## sort the ref_count from the smallest to the largest
            remove_list = list()
            removed_rdd_list = list()  # record for sticky policy
            for block in sorted_map:  # block_id, ref_count
                block_to_evict = machine.search_block_by_id(block[0])
                if block_to_evict.rdd_id not in removed_rdd_list:
                    if self.sticky:
                        blocks_of_the_rdd = machine.find_all_blocks_by_rdd_id(block_to_evict.rdd_id)
                        for block_of_the_rdd in blocks_of_the_rdd:
                            remove_list.append(block_of_the_rdd)
                            cleared_size += block_of_the_rdd.size
                            if cleared_size >= size:
                                for block_to_evict in remove_list:
                                    self.cache_log.write(
                                        "\t\tEvict -- Referenced Count + Sticky: %s\n" % block_to_evict.id)
                                    self.do_remove(machine, block_to_evict, time)
                                return True
                        removed_rdd_list.append(block_to_evict.rdd_id)
                    else:
                        if block_to_evict.rdd_id != rdd_id:
                            self.cache_log.write("\t\tEvict -- Referenced Count with Conservative Sticky: %s\n" % block_to_evict.id)
                            remove_list.append(block_to_evict)
                            cleared_size += block_to_evict.size
                            if cleared_size >= size:
                                for block_to_evict in remove_list:
                                    self.do_remove(machine, block_to_evict, time)
                                return True
        elif self.strict_sticky:
            cached_blocks = machine.cached_blocks  # cached_block_id
            ref_map = dict()  # (block_id, ref_count)
            for block in cached_blocks:  # will the recency be kept? seems not
                ref_map[block.id] = block.eff_ref_count_rdd
            sorted_map = sorted(ref_map.items(),
                                key=lambda x: x[1])  ## sort the ref_count from the smallest to the largest
            remove_list = list()
            removed_rdd_list = list()  # record for sticky policy
            for block in sorted_map:  # block_id, ref_count
                block_to_evict = machine.search_block_by_id(block[0])
                if block_to_evict.rdd_id != rdd_id and block_to_evict.rdd_id not in removed_rdd_list:
                    if self.sticky:
                        blocks_of_the_rdd = machine.find_all_blocks_by_rdd_id(block_to_evict.rdd_id)
                        for block_of_the_rdd in blocks_of_the_rdd:
                            remove_list.append(block_of_the_rdd)
                            cleared_size += block_of_the_rdd.size
                            if cleared_size >= size:
                                for block_to_evict in remove_list:
                                    self.cache_log.write(
                                        "\t\tEvict -- Referenced Count + Sticky: %s\n" % block_to_evict.id)
                                    self.do_remove(machine, block_to_evict, time)
                                return True
                        removed_rdd_list.append(block_to_evict.rdd_id)
                    else:
                        if block_to_evict.rdd_id != rdd_id:
                            self.cache_log.write(
                                "\t\tEvict -- Referenced Count with Strict Sticky: %s\n" % block_to_evict.id)
                            remove_list.append(block_to_evict)
                            cleared_size += block_to_evict.size
                            if cleared_size >= size:
                                for block_to_evict in remove_list:
                                    self.do_remove(machine, block_to_evict, time)
                                return True
        elif self.evict_dead:
            remove_list = list()
            # removed_rdd_list = list()  # record for sticky policy
            cached_blocks = machine.cached_blocks
            for block in cached_blocks:
                if block.ref_count == 0: # find all dead blocks and evict them at once
                    cleared_size += block.size
                    remove_list.append(block)
            if cleared_size >= size:
                for block_to_evict in remove_list:
                    self.cache_log.write("\t\tEvict -- Dead block: %s\n" % block_to_evict.id)
                    self.do_remove(machine, block_to_evict, time)
                return True
            for block in machine.cached_blocks:
                if block.rdd_id != rdd_id and block.ref_count != 0: # those dead blocks have already been added
                    remove_list.append(block)
                    cleared_size += block.size
                    if cleared_size >= size:
                        for block_to_evict in remove_list:
                            self.cache_log.write("\t\tEvict -- LRU: %s\n" % block_to_evict.id)
                            self.do_remove(machine, block_to_evict, time)
                        return True
        return False

    def search_machine_by_id(self, id):
        for machine in self.machines:
            if machine.id == id:
                return machine
        return False

    def search_rdd_by_id(self, id):
        for rdd in self.rdd_list:
            if rdd.id == id:
                return rdd
        return False

    def check_incomplete_rdd(self, rdd_id, user_id): # check whether this rdd is complete or not and update self.incomplete_rdd_ids
        count = 0
        for block_id in self.cache_map:  # complete rdd: all of its partitions are in memory
            machine = self.search_machine_by_id(self.cache_map[block_id]) # find the machine caching this block
            block = machine.search_block_by_id(block_id) # find this block
            if block.rdd_id == rdd_id:
                count += 1
        if count == self.search_rdd_by_id(rdd_id).partitions:# complete rdd
            if rdd_id in self.incomplete_rdd_ids:
                self.incomplete_rdd_ids.remove(rdd_id)
        elif rdd_id not in self.incomplete_rdd_ids: # incomplete rdd
            self.incomplete_rdd_ids.append(rdd_id)
        return

    def get_percentage_of_dead_cache(self):

        dead_size = 0.0
        for block_id in self.cache_map: # block id
            machine = self.search_machine_by_id(self.cache_map[block_id])
            block = machine.search_block_by_id(block_id)
            if block.ref_count == 0:
                dead_size += block.size
        total_size = len(self.machines)*self.machines[0].cache_size
        return dead_size/total_size

    def get_percentage_of_used_cache(self):
        '''
        used_size = 0.0
        for block_id in self.cache_map: # block id
            machine = self.search_machine_by_id(self.cache_map[block_id])
            block = machine.search_block_by_id(block_id)
            used_size += block.size
        '''
        total_size = len(self.machines) * self.machines[0].cache_size
        remaining_size = 0.0
        for machine in self.machines:
            remaining_size += machine.remaining_cache_size
        return 1 - remaining_size/total_size


    def reset(self):
        self.access_pattern.write("\n\n")
        self.cache_map = dict()  # all the blocks that are cached in the cluster, map: (block_id, machine_id)
        self.incomplete_rdd_ids = list()
        self.lru = False
        self.ref_count = False
        self.lfu = False
        self.evict_dead = False
        self.sticky = False  # True or False
        self.conservative_sticky = False
        self.strict_sticky = False
        #self.runtime_ref = dict()  # runtime referenced count, map:(block_id, ref_count)
        cache_log_dir = os.getcwd()  ## get current working directory
        self.cache_log = open("%s/cache_log.txt" % cache_log_dir, "w")  # log for tracking the cache status


