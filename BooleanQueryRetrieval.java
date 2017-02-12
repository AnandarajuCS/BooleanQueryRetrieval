

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

/**
 * BooleanQueryRetrieval class creates the Inverted index using the indexed values.
 * This class uses the lucene core jar for index reading purposes.
 * Additional methods includes, finding Term-at-a-time OR, Term-at-a-time AND, Document-at-a-time OR and Document-at-a-time AND 
 * for a set of query terms.
 * 
 * @author Anandaraju Coimbatore Sivaraju
 * UBITName : anandara
 * UB Person number : 50206340
 *
 */
public class BooleanQueryRetrieval{

	public static String pathOfIndex="";
	public static String outputFile = "";
	public static String inputFile = "";
	public static ArrayList<String[]> inputTerms = new ArrayList<String[]>();
	/**
	 * postingsList - used to store the Inverted index created from the given index.
	 */
	public static Map<String,LinkedList<Integer>> postingsList = new HashMap<String,LinkedList<Integer>>();
	public static BufferedWriter outputFileWriter = null;
	public static int taatORComparison=0;
	public static int taatANDComparison=0;

	public static void main(String[] args) {
		if(args.length != 3) 
		{
			System.out.println("Enter valid arguments: <path_of_index> <output.txt> <input.txt>");
			return;
		}

		pathOfIndex = args[0];
		outputFile = args[1];
		inputFile = args[2];

		File file = new File(outputFile);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			outputFileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), Charset.forName( "UTF-8" )));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// code starts here
		try {
			createInvertedIndex();
			readInputFile();

			//call the methods for terms in each line, one by one
			Iterator<String[]> termIterator = inputTerms.iterator();
			while(termIterator.hasNext())
			{
				String[] termArray = termIterator.next(); // String array containing the individual terms of first line and so on.
				getPostings(termArray);
				taatAND(termArray);
				taatOR(termArray);
				daatAND(termArray);
				daatOR(termArray);
			}
		} catch (IOException e) {
			try {
				outputFileWriter.flush();
				outputFileWriter.close();
				System.out.println();
				System.out.println("The final results are written into " + outputFile + " file");
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}

		try {
			outputFileWriter.flush();
			outputFileWriter.close();
			System.out.println();
			System.out.println("The final results are written into " + outputFile + " file");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createInvertedIndex() throws IOException
	{
		Directory directory;
		IndexReader indexReader=null;
		try {
			directory = FSDirectory.open(Paths.get(pathOfIndex));
			indexReader = DirectoryReader.open(directory);
		} catch (IOException e) {
			e.printStackTrace();
		}
		ArrayList<String> termList = new ArrayList<String>();
		Collection<String> fields = MultiFields.getIndexedFields(indexReader);
		for(String field:fields)
		{
			if( field.equals("id") || field.equals("_version_") )
			{
				continue;
			}
			Terms terms = MultiFields.getTerms(indexReader, field);
			TermsEnum iterator = terms.iterator();
			BytesRef byteRef = null;
			while((byteRef = iterator.next()) != null) {
				String term = byteRef.utf8ToString();
				termList.add(term);
				PostingsEnum postingsEnum = MultiFields.getTermDocsEnum(indexReader, field, byteRef);
				while((postingsEnum.nextDoc() != PostingsEnum.NO_MORE_DOCS))
				{
					if(postingsList.get(term) != null)
					{
						postingsList.get(term).add(postingsEnum.docID());
					}
					else
					{
						LinkedList<Integer> posting =  new LinkedList<Integer>();
						posting.add(postingsEnum.docID());
						postingsList.put(term,posting);
					}
				}
			}
		}
	}

	/**
	 * readInputFile method is used to read the query terms from the input file.
	 * @throws IOException
	 */
	public static void readInputFile() throws IOException
	{
		BufferedReader br = new BufferedReader(
				new InputStreamReader(new FileInputStream(inputFile),"utf-8"));
		System.out.println();
		System.out.println("Processing the below query terms : ");
		try {
			String line;
			while ((line = br.readLine()) != null) {
				String[] terms = line.split(" ");
				inputTerms.add(terms);
				System.out.println(line);
			}
		} finally {
			br.close();
		}
	}

	/**
	 * getPostings method is used to get the postings for the query terms
	 * @param termArray
	 * @throws IOException
	 */
	public static void getPostings(String[] termArray) throws IOException
	{
		for(String term :termArray)
		{
			outputFileWriter.write("GetPostings");
			outputFileWriter.newLine();
			outputFileWriter.write(term);
			outputFileWriter.newLine();
			outputFileWriter.write("Postings list:" + printList(postingsList.get(term)));  // we need to write the TaatAND Results to output file.
			outputFileWriter.newLine();
		}
	}

	public static void taatOR(String[] termArray) throws IOException
	{
		outputFileWriter.write("TaatOr");
		outputFileWriter.newLine();
		if(termArray.length == 0)
			return ;

		LinkedList<Integer> partialList = null;
		for(int i = 0; i<termArray.length;i++)
		{
			outputFileWriter.write(termArray[i] + " ");
			if(partialList == null)
			{
				partialList=(LinkedList<Integer>)postingsList.get(termArray[i]).clone();
			}else
			{
				partialList=mergeLists(partialList,(LinkedList<Integer>)postingsList.get(termArray[i]).clone());
			}
		}
		outputAppender(partialList, taatORComparison);
		taatORComparison=0; //resetting the comparison counter
	}

	/**
	 * mergeLists - used to union two lists.
	 * @param a
	 * @param b
	 * @return
	 */
	public static LinkedList<Integer> mergeLists(LinkedList<Integer> a,LinkedList<Integer> b)
	{
		int i = 0,j=0;
		while(i<a.size() && j < b.size())
		{
			if(a.get(i) > b.get(j))
			{
				a.add(i, b.get(j));
				i++;
				j++;
			}else if (a.get(i) < b.get(j))
			{
				i++;
			}else
			{
				i++;
				j++;
			}
			taatORComparison++;
		}
		if(j<b.size())
		{
			for(int m = j ; m<b.size();m++)
			{
				a.add(b.get(m));
			}
		}
		return a;
	}

	public static void taatAND(String[] termArray) throws IOException
	{
		outputFileWriter.write("TaatAnd");
		outputFileWriter.newLine();
		if(termArray.length == 0)
			return ;

		LinkedList<Integer> partialList = null;
		for(int i = 0; i<termArray.length;i++)
		{
			outputFileWriter.write(termArray[i] + " ");
			if(partialList == null)
			{
				partialList=(LinkedList<Integer>)postingsList.get(termArray[i]).clone();
			}else
			{
				partialList=intersectLists(partialList,(LinkedList<Integer>)postingsList.get(termArray[i]).clone());
			}
		}
		outputAppender(partialList, taatANDComparison);
		taatANDComparison=0; //resetting the comparison counter
	}

	/**
	 * intersectLists - used to find the intersection of the two lists
	 * @param a
	 * @param b
	 * @return
	 */
	public static LinkedList<Integer> intersectLists(LinkedList<Integer> a, LinkedList<Integer> b)
	{
		int i =0,j=0;
		LinkedList<Integer> c = new LinkedList<Integer>();
		while(i<a.size() && j<b.size())
		{
			if(a.get(i) < b.get(j))
			{
				i++;
			}else if (a.get(i) > b.get(j))
			{
				j++;
			}
			else{
				c.add(a.get(i));
				i++;
				j++;
			}
			taatANDComparison++;
		}
		return c;
	}

	public static void daatAND(String[] termArray) throws IOException
	{
		outputFileWriter.write("DaatAnd");
		outputFileWriter.newLine();
		Map<String,LinkedList<Integer>> intermediatePostings= new HashMap<String,LinkedList<Integer>>();		
		LinkedList<Integer> outputPostingList = new LinkedList<Integer>();
		for(String qTerm : termArray)
		{
			outputFileWriter.write(qTerm + " ");
			intermediatePostings.put(qTerm,(LinkedList<Integer>)postingsList.get(qTerm).clone());
		}// generated the intermediate postingList containing only the postings for the query terms

		int initialSize = intermediatePostings.size();
		int modSize = initialSize;
		int comparison=0;

		if(initialSize == 1)
		{
			outputPostingList=(LinkedList<Integer>)postingsList.get(termArray[0]).clone();
		}else
		{
			outer_loop:
				while(modSize == initialSize)  // even if one of the term's posting list is made empty then, we need to stop, since it is AND
				{
					int maxDoc = 0;
					ArrayList<String> maxDocList = new ArrayList<String>();
					Iterator<String> pListIterator = intermediatePostings.keySet().iterator();
					while(pListIterator.hasNext())
					{
						String currentTerm = pListIterator.next();
						LinkedList<Integer> currentPostingsList = intermediatePostings.get(currentTerm);
						if(currentPostingsList.peek() > maxDoc)
						{
							//poll the maxlist terms
							if(!maxDocList.isEmpty())
							{
								Iterator<String> maxDocIterator = maxDocList.iterator();
								while(maxDocIterator.hasNext())
								{
									String current = maxDocIterator.next();
									intermediatePostings.get(current).poll();
									if(intermediatePostings.get(current).isEmpty())
									{
										//intermediatePostings.remove(current);
										modSize--;
										break outer_loop;
									}
								}
								maxDocList.clear();
							}
							maxDoc = currentPostingsList.peek();
							maxDocList.add(currentTerm);
						}else if (currentPostingsList.peek() == maxDoc)
						{
							maxDocList.add(currentTerm);
						}else
						{
							currentPostingsList.poll(); // removing the docId
							if(intermediatePostings.get(currentTerm).isEmpty())
							{
								//intermediatePostings.remove(currentTerm);
								modSize--;
							}
						}
						comparison++;
					}// now got the least doc number among all the qterm postings list

					if(maxDocList.size() == initialSize)
					{
						outputPostingList.add(maxDoc); // add the doc id to the output postings list
						Iterator<String> maxDocIterator = maxDocList.iterator();
						while(maxDocIterator.hasNext())
						{
							String current = maxDocIterator.next();
							intermediatePostings.get(current).poll();
							if(intermediatePostings.get(current).isEmpty())
							{
//								intermediatePostings.remove(current);
								modSize--;
							}
						}
					}
					// now we need to poll all the minDocList documents in intermediate postings list to get ready to iterate over again.
				}
		}
		outputAppender(outputPostingList, comparison);
	}

	public static void daatOR(String[] termArray) throws IOException
	{
		outputFileWriter.write("DaatOr");
		outputFileWriter.newLine();
		Map<String,LinkedList<Integer>> intermediatePostings= new HashMap<String,LinkedList<Integer>>();
		LinkedList<Integer> outputPostingList = new LinkedList<Integer>();
		for(String qTerm : termArray)
		{
			outputFileWriter.write(qTerm + " ");
			intermediatePostings.put(qTerm,(LinkedList<Integer>)postingsList.get(qTerm).clone());
		}// generated the intermediate postingList containing only the postings for the query terms

		int comparison=0;

		while(intermediatePostings.size() > 1)
		{
			int minDoc = Integer.MAX_VALUE;
			ArrayList<String> minDocList = new ArrayList<String>();
			Iterator<String> pListIterator = intermediatePostings.keySet().iterator();
			while(pListIterator.hasNext())
			{
				String currentTerm = pListIterator.next();
				LinkedList<Integer> currentPostingsList = intermediatePostings.get(currentTerm);
				if(currentPostingsList.peek() < minDoc)
				{
					minDocList.clear();
					minDoc = currentPostingsList.peek();
					minDocList.add(currentTerm);
					comparison++;
				}else if (currentPostingsList.peek() == minDoc)
				{
					minDocList.add(currentTerm);
					comparison++;
				}
			}// now we got the least doc number among all the query term postings list

			outputPostingList.add(minDoc); // add the doc id to the output postings list

			// now we need to poll all the minDocList documents in intermediate postings list to get ready to iterate over again.
			Iterator<String> minDocIterator = minDocList.iterator();
			while(minDocIterator.hasNext())
			{
				String current = minDocIterator.next();
				intermediatePostings.get(current).poll();
				if(intermediatePostings.get(current).isEmpty())
				{
					intermediatePostings.remove(current);
				}
			}
		}
		if(intermediatePostings.size() > 0)
		{
			outputPostingList.addAll(intermediatePostings.get(intermediatePostings.keySet().iterator().next()));
		}
		outputAppender(outputPostingList, comparison);
	}

	/**
	 * printList - to format the output postings list in a space separator.
	 * @param list
	 * @return
	 */
	public static String printList(LinkedList<Integer> list)
	{
		String results="";
		for(int val:list)
		{
			results+=" "+String.valueOf(val);
		}
		return results;
	}

	/**
	 * outputAppender - send the final results to the outputFileWriter
	 * @param list
	 * @param comparison
	 * @throws IOException
	 */
	public static void outputAppender(LinkedList<Integer> list, int comparison) throws IOException
	{
		outputFileWriter.newLine();
		outputFileWriter.write("Results:" + ((list.size()>0)? printList(list):" empty"));
		outputFileWriter.newLine();
		outputFileWriter.write("Number of documents in results: " + list.size());
		outputFileWriter.newLine();
		outputFileWriter.write("Number of comparisons: " + comparison);
		outputFileWriter.newLine();
	}
}
