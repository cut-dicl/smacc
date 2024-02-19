package edu.cut.smacc.server.cache.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class is used in order to give information of
 * existing and non-existing blocks in order to know which ranges of block to create
 *
 * @author Theodoros Danos
 */
public class MultiBlockUnifier {

    private ArrayList<BlockRange> unifiedList;

    public MultiBlockUnifier(List<BlockRange> ranges) {
        HashMap<String, ArrayList<Long>> map = createBlockUnionMap(ranges);
        unifiedList = createUnionBlockRangeList(map);
    }

    /**
     * This method helps to create a map so we can unify the range of blocks together
     * <p>
     * In each existing range in reservedRange List, it's boundaries  (start, stop)
     * are added to the map (as keys). Each key is pointing to a list having all
     * concatenated boundaries.
     * <p>
     * For example assume the reservedRanges = [ [1,5],[6,9],[10,15],[50,60] ]
     * The map will contain the keys 1,5,6,9,10,15 having as value pointers to the same list that has
     * all the keys that pointing to this list
     *
     * @param ranges
     * @return the map created from reserved (from other or previous operations of same CacheFile) ranges
     */
    private HashMap<String, ArrayList<Long>> createBlockUnionMap(List<BlockRange> ranges) {
        HashMap<String, ArrayList<Long>> map = new HashMap<>();

        for (BlockRange range : ranges) {
            String lookingStartIndex = String.valueOf(range.getStart() - 1);
            String lookingStopIndex = String.valueOf(range.getStop() + 1);
            String startIndex = String.valueOf(range.getStart());
            String stopIndex = String.valueOf(range.getStop());

            ArrayList<Long> currentRangeList;
            if (map.containsKey(lookingStartIndex) && map.containsKey(lookingStopIndex)) {
                //union of two sets
                currentRangeList = map.get(lookingStartIndex);    //get list from one side
                ArrayList<Long> oldRangeList = map.get(lookingStopIndex);    //get list from other side

                //update all pointers to this list
                for (Long kRange : oldRangeList) {
                    String key = String.valueOf(kRange);
                    ArrayList<Long> temp = map.get(key);
                    if (temp == oldRangeList) map.put(key, currentRangeList);
                }

                currentRangeList.addAll(oldRangeList);    //unify the lists
                oldRangeList.clear(); //clear old and now obsolete list
            } else if (map.containsKey(lookingStartIndex)) currentRangeList = map.get(lookingStartIndex);
            else if (map.containsKey(lookingStopIndex)) currentRangeList = map.get(lookingStopIndex);
            else currentRangeList = new ArrayList<>();

            currentRangeList.add(range.getStart());
            currentRangeList.add(range.getStop());
            map.put(stopIndex, currentRangeList);
            map.put(startIndex, currentRangeList);
        }

        return map;
    }

    private ArrayList<BlockRange> createUnionBlockRangeList(HashMap<String, ArrayList<Long>> map) {
        ArrayList<ArrayList<Long>> lists = new ArrayList<>(map.values());
        ArrayList<BlockRange> unified = new ArrayList<>();
        for (ArrayList<Long> list : lists) {
            long max = list.get(0);
            long min = list.get(0);
            for (Long i : list) {
                if (i < min) min = i;
                if (i > max) max = i;
            }
            unified.add(new BlockRange(min, max));
        }
        return unified;
    }

    public ArrayList<BlockRange> getUnifiedList() {
        return unifiedList;
    }

    public BlockRange findBlockContainsRange(long r) {
        for (BlockRange range : unifiedList) {
            if (range.contains(r)) return range;
        }
        return null;
    }

    public BlockRange findEmptySpace(BlockRange rangeContainsStart, long curStart, long finalStop) {
        long emptySpaceStart, emptySpaceStop;

        if (rangeContainsStart == null)
            emptySpaceStart = curStart;
        else
            emptySpaceStart = rangeContainsStart.getStop() + 1;

        long minStop = -1;

        for (BlockRange range : unifiedList) {
            if (range.getStart() >= emptySpaceStart)
                if (range.getStart() < minStop || minStop == -1) minStop = range.getStart();
        }

        if (minStop >= 0)
            emptySpaceStop = minStop - 1;
        else
            emptySpaceStop = finalStop;

        return new BlockRange(emptySpaceStart, emptySpaceStop);
    }
}
