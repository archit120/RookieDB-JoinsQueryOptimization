package edu.berkeley.cs186.database.query.join;

import java.io.Console;
import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Table;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource, QueryOperator rightSource, String leftColumnName,
            String rightColumnName, TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction), leftColumnName, rightColumnName, transaction,
                JoinType.BNLJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        // This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages
                + getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join. Look
     * over the implementation in SNLJOperator if you want to get a feel for the
     * fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record> {
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private int maxRecords;
        private int currentPos;
        private int maxPos;
        private List<Record> blockRecords;
        private boolean lastIncomplete;

        private int li;
        private int ri;
        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;

            this.maxRecords = getNumberOfCombinedRecords() - 1;
            this.maxPos = 0;
            this.currentPos = 0;
            this.blockRecords = new ArrayList<>(maxRecords);
            this.lastIncomplete = false;
            while (blockRecords.size() < maxRecords)
                blockRecords.add(null);

            li =0; ri=0;
        }

        private Boolean hasNextLeftBlock() {
            // System.out.println("left check");
            return this.leftSourceIterator.hasNext();
        }

        private Boolean hasNextRightPage() {
            // System.out.println("right check");

            return this.rightSourceIterator.hasNext();
        }

        private int InnerRightLoop(int currentPos) {
            while (rightPageIterator.hasNext() && currentPos < maxRecords) {
                Record rightRecord = rightPageIterator.next();
                
                // if(li %100 == 0)
                // System.out.println((li-1)+ " " + ri + " " + numBuffers);
                ri++;
                
                if (compare(leftRecord, rightRecord) == 0) {
                    blockRecords.set(currentPos++, leftRecord.concat(rightRecord));
                }
            }
            return currentPos;
        }

        private boolean MergeOnePage() {
            currentPos = 0;

            // boolean rightIncomplete = hasNextRightPage() || lastIncomplete;
            if (lastIncomplete || leftBlockIterator.hasNext() || hasNextRightPage()) {
                
                if (lastIncomplete) {
                    currentPos = InnerRightLoop(currentPos);
                    lastIncomplete = false;
                    if (rightPageIterator.hasNext())
                        lastIncomplete = true;

                }
                else {
                    rightPageIterator.reset();

                    if (!leftBlockIterator.hasNext()) {
                        fetchNextRightPage();
                        leftBlockIterator.reset();
                    }

                    while (leftBlockIterator.hasNext() && currentPos < maxRecords) {
                        leftRecord = leftBlockIterator.next();
                        li++;
                        currentPos = InnerRightLoop(currentPos);
                        if (rightPageIterator.hasNext())
                            lastIncomplete = true;
                        else
                            rightPageIterator.reset();
                    }
                }

            } else if (hasNextLeftBlock()) {
                rightSourceIterator.reset();
                fetchNextLeftBlock();
                fetchNextRightPage();
                return MergeOnePage();
            } else {
                maxPos = currentPos = 0;
                return false;
            }

            maxPos = currentPos;
            currentPos = 0;
            return true;
        }

        /**
         * Fetch the next block of records from the left source. leftBlockIterator
         * should be set to a backtracking iterator over up to B-2 pages of records from
         * the left source, and leftRecord should be set to the first record in this
         * block.
         *
         * If there are no more records in the left source, this method should do
         * nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextLeftBlock() {
            if (hasNextLeftBlock()) {
                // System.out.println("fetching new left page");
                this.leftBlockIterator = QueryOperator.getBlockIterator(this.leftSourceIterator,
                        getLeftSource().getSchema(), numBuffers - 2);
                this.leftBlockIterator.markNext();
            }
        }

        /**
         * Fetch the next page of records from the right source. rightPageIterator
         * should be set to a backtracking iterator over up to one page of records from
         * the right source.
         *
         * If there are no more records in the right source, this method should do
         * nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextRightPage() {
            if (hasNextRightPage()) {
                // System.out.println("fetching new right page");

                this.rightPageIterator = QueryOperator.getBlockIterator(this.rightSourceIterator,
                        getRightSource().getSchema(), 1);
                this.rightPageIterator.markNext();
            }
        }

        /**
         * Returns the next record that should be yielded from this join, or null if
         * there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare function
         * directly from this file, since BNLJOperator is a subclass of JoinOperator).
         */
        private Record fetchNextRecord() {
            boolean ret = true;
            while (maxPos == currentPos && ret)
                ret = MergeOnePage();

            if (!ret) {
                return null;
            }

            return blockRecords.get(currentPos++);
        }

        /**
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null)
                this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext())
                throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
