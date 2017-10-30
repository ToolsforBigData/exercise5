from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep

# Find out if a graph is a euler tour or not
class MRIsEuler(MRJob):

    # We define the ouput protocol as json so we don't print out null
    # when we yield None 
    OUTPUT_PROTOCOL = JSONValueProtocol
    
    def mapper_nodes(self, _, line):
        #Split up the edge connections (0,1) edge to [(0,1),(1,1)]
        for node in line.split():
            yield (node, 1)

    def reducer_count(self, node, counts):
        # Check if specific node has odd or even edge connected to it.
        # and yield the same value for key as the next reducer won't
        # print out two times.
       if (sum(counts) % 2) != 0:
           yield ("key",1)
       else:
           yield("key",0)

    def reducer_isEuler(self,key,counts):
        # Print out the result if a graph has an Euler tour or not.

        # This reducer takes every yield out result from the other reducer
        # and sums up the counts. As we only have one key from the previous reducer
        # we will sum everything node together. If the any of the node has any odd number
        # of edges then the sum of counts will be larger than 0 and print out that this 
        # specific graph is not a euler tour.
        if sum(counts) == 0:
            yield (None,"Graph has Euler tour")
        else:
            yield(None,"Graph has not a Euler tour")


    def steps(self):
        # We have two reduce therefore we have multiple steps and we need to override
        # steps()
        return [
                MRStep( mapper=self.mapper_nodes,
                        reducer=self.reducer_count),
                MRStep(reducer=self.reducer_isEuler)
        ]

if __name__ == '__main__':
    MRIsEuler.run()




