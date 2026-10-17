package com.bmo.sftr.mapping.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

/**
 * Second step of the Kafka route ({@code process: ref: offsetCommitProcessor}). Runs on the consumer thread
 * right after each message and commits every offset whose row is on disk.
 */
@Component("offsetCommitProcessor")
public class OffsetCommitProcessor implements Processor {

    private final OffsetCommitter committer;

    public OffsetCommitProcessor(OffsetCommitter committer) {
        this.committer = committer;
    }

    @Override
    public void process(Exchange exchange) {
        committer.commitReleased();
    }
}
