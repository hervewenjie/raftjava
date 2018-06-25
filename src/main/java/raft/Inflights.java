package raft;

import lombok.Builder;
import util.Panic;


/**
 * inflights is a sliding window for the inflight messages.
 * Each inflight message contains one or more log entries.
 * The max number of entries per message is defined in raft config as MaxSizePerMsg.
 * Thus inflight effectively limits both the number of inflight messages
 * and the bandwidth each Progress can use.
 * When inflights is full, no more message should be sent.
 * When a leader sends out a message, the index of the last
 * entry should be added to inflights. The index MUST be added
 * into inflights in order.
 * When a leader receives a reply, the previous inflights should
 * be freed by calling inflights.freeTo with the index of the last
 *
 * Created by chengwenjie on 2018/5/30.
 */
@Builder
public class Inflights {

    // the starting index in the buffer
    int start;
    // number of inflights in the buffer
    int count;

    // the size of the buffer
    int size;

    // buffer contains the index of the last entry
    // inside one message.
    @Builder.Default
    long[] buffer = {};

    public static Inflights newInflights(int size) {
        return Inflights.builder().size(size).build();
    }

    // full returns true if the inflights is full.
    public boolean full() {
        return count == size;
    }

    // add adds an inflight into inflights
    public void add(long inflight) {
        if (full()) {
            Panic.panic("cannot add into a full inflights");
        }
        int next = start + count;
        int size = this.size;
        if (next >= size) {
            next -= size;
        }
        if (next >= buffer.length) {
            growBuf();
        }
        buffer[next] = inflight;
        count++;
    }

    // resets frees all inflights.
    public void reset() {
        count = 0;
        start = 0;
    }

    // freeTo frees the inflights smaller or equal to the given `to` flight.
    public void freeTo(Long to) {
        if (count == 0 || to < buffer[start]) {
            // out of the left side of the window
            return;
        }
        int idx = start;
        int i;
        for (i = 0; i < count; i++) {
            if (to < buffer[idx]) {
                // found the first large inflight
                break;
            }

            // increase index and maybe rotate
            int size = this.size;
            idx++;
            if (idx > size) {
                idx -= size;
            }
        }
        // free i inflights and set new start index
        count -= i;
        start = idx;
        if (count == 0) {
            // inflights is empty, reset the start index so that we don't grow the
            // buffer unnecessarily.
            start = 0;
        }
    }

    public void freeFirstOne() {
        freeTo(buffer[start]);
    }

    // - - - -
    // privates

    // grow the inflight buffer by doubling up to inflights.size. We grow on demand
    // instead of preallocating to inflights.size to handle systems which have
    // thousands of Raft groups per process.
    private void growBuf() {
        int newSize = buffer.length * 2;
        if (newSize == 0) {
            newSize = 1;
        } else if (newSize > size) {
            newSize = size;
        }
        long[] newBuffer = new long[newSize];
        for (int i = 0; i < buffer.length; i++) newBuffer[i] = buffer[i];
        buffer = newBuffer;
    }


}
