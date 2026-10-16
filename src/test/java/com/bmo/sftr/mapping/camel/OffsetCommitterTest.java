package com.bmo.sftr.mapping.camel;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class OffsetCommitterTest {

    private final OffsetCommitter committer = new OffsetCommitter();
    private final List<String> commits = new ArrayList<>();

    private Runnable commit(String name) {
        return () -> commits.add(name);
    }

    @Test
    void nothingIsCommittedUntilTheConsumerThreadAsks() {
        committer.release("t-0", 5, commit("t-0@5"));
        assertThat(commits).isEmpty();
        assertThat(committer.waiting()).isEqualTo(1);
        assertThat(committer.commitReleased()).isEqualTo(1);
        assertThat(commits).containsExactly("t-0@5");
        assertThat(committer.waiting()).isZero();
    }

    @Test
    void onlyTheHighestOffsetPerPartitionIsCommitted() {
        committer.release("t-0", 5, commit("t-0@5"));
        committer.release("t-0", 7, commit("t-0@7"));
        committer.release("t-0", 6, commit("t-0@6"));           // late and lower: ignored
        committer.release("t-1", 2, commit("t-1@2"));

        assertThat(committer.commitReleased()).isEqualTo(2);
        assertThat(commits).containsExactlyInAnyOrder("t-0@7", "t-1@2");
    }

    @Test
    void aSecondCallHasNothingLeftToCommit() {
        committer.release("t-0", 1, commit("t-0@1"));
        committer.commitReleased();
        assertThat(committer.commitReleased()).isZero();
        assertThat(commits).hasSize(1);
    }

    @Test
    void aFailingCommitDoesNotStopTheOthers() {
        committer.release("t-0", 1, () -> { throw new IllegalStateException("broker gone"); });
        committer.release("t-1", 1, commit("t-1@1"));
        assertThat(committer.commitReleased()).isEqualTo(1);
        assertThat(commits).containsExactly("t-1@1");
    }

    @Test
    void anotherThreadMayReleaseWhileTheConsumerCommits() throws Exception {
        Thread flusher = Thread.ofVirtual().start(() -> committer.release("t-0", 9, commit("t-0@9")));
        flusher.join();
        assertThat(committer.commitReleased()).isEqualTo(1);
        assertThat(commits).containsExactly("t-0@9");
    }
}
