from mrjob.job import MRJob 
from mrjob.step import MRStep
import heapq


class MRAveMovieScore(MRJob):

	#You can either overwrite classes or define them in the steps
	#Here we define steps because we have 2 reduce steps
	def steps(self):
		return [
			MRStep(mapper=self.mapperA,
				   reducer=self.reducerA),
			MRStep(reducer=self.reducer_find_top20)
		]
		
	def mapperA(self, _,line):
		userid,score,title  = line.split("|")
		title.encode('utf-8')

		#Example  (Toy Story, (4.0 , 1))
		yield (title,(float(score),1))

	def comp_ave(self,title,scores):
		#For each title
		ave = 0
		counter = 0

		#iterating on both the score and the counter
		for score,i in scores:
			#Updating the average on every loop
			ave = (ave*counter + score*i) / (counter + i)
			counter += i

		return (title, (ave,counter))

	def reducerA(self, title, scores):
		#"Toy Story", (4.5 average, 150 users rated) 
		title, (ave, count) = self.comp_ave(title, scores)
		#Only take if at least 100 users have rated.
		if count >= 100:
			#None, (4.2562, "Toy Story")
			yield None,(round(ave,4), title)

	# Discard the key; it is just None
	def reducer_find_top20(self, _, scores_pair):
		# each item of word_count_pairs is (count, word),
		# so yielding one results in key=counts, value=word
		return heapq.nlargest(10, scores_pair)

if __name__ == '__main__':
    MRAveMovieScore.run()
