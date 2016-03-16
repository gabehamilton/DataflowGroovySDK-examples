package com.tempusvolat.dataflow


import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.Count
import com.google.cloud.dataflow.sdk.transforms.Filter
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements
import com.google.cloud.dataflow.sdk.transforms.MapElements
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction
import com.google.cloud.dataflow.sdk.values.KV
import com.google.cloud.dataflow.sdk.values.TypeDescriptor

/**
 * An example that counts words in Shakespeare, using Groovy
 *
 * <p>See {@link MinimalWordCount} for a comprehensive explanation.
 */
public class Main {

	public static void main(String[] args) {
		Map commandLineArgs = [:]
		args.each {
			String[] parts = it.split('=')
			commandLineArgs.put(parts[0], parts[1])
		}
		if (!commandLineArgs.project || !commandLineArgs.stagingLocation || !commandLineArgs.outputLocation) {
			println "Usage: gradle run -Pargs=\"project=YOUR_PROJECT_NAME stagingLocation=gs://YOUR_BUCKET\""
			return
		}

		DataflowPipelineOptions options = PipelineOptionsFactory.create()
				.as(DataflowPipelineOptions)

		options.setRunner(BlockingDataflowPipelineRunner)

		options.setProject(commandLineArgs.project)
		options.setNumWorkers(3)

		options.setStagingLocation("gs://" + commandLineArgs.stagingLocation)

		SerializableFunction splitWords = { String word ->
			Arrays.asList(word.split("[^a-zA-Z']+"))
		}

		Filter emptyWordsFilter = Filter.byPredicate({String word -> !word.isEmpty()})

		SerializableFunction mapFunction = { KV<String, Long> wordCount ->
			wordCount.getKey() + ": " + wordCount.getValue()
		}
		Pipeline p = Pipeline.create(options)

		p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/*"))

				.apply(FlatMapElements.via(splitWords)  .withOutputType(new TypeDescriptor<String>() {}))

				.apply(emptyWordsFilter)

				.apply(Count.<String>perElement())

				.apply(MapElements.via(mapFunction) .withOutputType(new TypeDescriptor<String>() {}))

				.apply(TextIO.Write.to("gs://" + (commandLineArgs.outputLocation)))

		p.run()
	}



}