package Conf;

import java.io.Serializable;
import java.util.Comparator;

import Application.WordCount;
import MapRed.Input.TextInputFormat;
import MapRed.Output.SequenceFileOutputFormat;
import Utility.StringComparator;

@SuppressWarnings("rawtypes")
public class Configuration implements Serializable {
	
	private static final long serialVersionUID = 12L;
	
	private Class mapperClass = WordCount.Map.class;
	private Class reducerClass = WordCount.Reduce.class;
	private Class combinerClass = WordCount.Reduce.class;
	private Class inputFormatClass = TextInputFormat.class;
	private Class outputFormatClass = SequenceFileOutputFormat.class;
	private Class inputKeyClass = Long.class;
	private Class inputValueClass = String.class;
	private Class outputKeyClass = String.class;
	private Class outputValueClass = String.class;
	private int numReducers = 1;
	private int numMapper = 4;
	
	private Comparator comparator = new StringComparator();
	
	/**
	 * @return the mapperClass
	 */
	public Class getMapperClass() {
		return mapperClass;
	}
	/**
	 * @param mapperClass the mapperClass to set
	 */
	public void setMapperClass(Class mapperClass) {
		this.mapperClass = mapperClass;
	}
	/**
	 * @return the reducerClass
	 */
	public Class getReducerClass() {
		return reducerClass;
	}
	/**
	 * @param reducerClass the reducerClass to set
	 */
	public void setReducerClass(Class reducerClass) {
		this.reducerClass = reducerClass;
	}
	/**
	 * @return the combinerClass
	 */
	public Class getCombinerClass() {
		return combinerClass;
	}
	/**
	 * @param combinerClass the combinerClass to set
	 */
	public void setCombinerClass(Class combinerClass) {
		this.combinerClass = combinerClass;
	}
	/**
	 * @return the inputFormatClass
	 */
	public Class getInputFormatClass() {
		return inputFormatClass;
	}
	/**
	 * @param inputFormatClass the inputFormatClass to set
	 */
	public void setInputFormatClass(Class inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}
	/**
	 * @return the outputFormatClass
	 */
	public Class getOutputFormatClass() {
		return outputFormatClass;
	}
	/**
	 * @param outputFormatClass the outputFormatClass to set
	 */
	public void setOutputFormatClass(Class outputFormatClass) {
		this.outputFormatClass = outputFormatClass;
	}
	/**
	 * @return the inputKeyClass
	 */
	public Class getInputKeyClass() {
		return inputKeyClass;
	}
	/**
	 * @param inputKeyClass the inputKeyClass to set
	 */
	public void setInputKeyClass(Class inputKeyClass) {
		this.inputKeyClass = inputKeyClass;
	}
	/**
	 * @return the inputValueClass
	 */
	public Class getInputValueClass() {
		return inputValueClass;
	}
	/**
	 * @param inputValueClass the inputValueClass to set
	 */
	public void setInputValueClass(Class inputValueClass) {
		this.inputValueClass = inputValueClass;
	}
	/**
	 * @return the outputKeyClass
	 */
	public Class getOutputKeyClass() {
		return outputKeyClass;
	}
	/**
	 * @param outputKeyClass the outputKeyClass to set
	 */
	public void setOutputKeyClass(Class outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}
	/**
	 * @return the outputValueClass
	 */
	public Class getOutputValueClass() {
		return outputValueClass;
	}
	/**
	 * @param outputValueClass the outputValueClass to set
	 */
	public void setOutputValueClass(Class outputValueClass) {
		this.outputValueClass = outputValueClass;
	}
	
	public Comparator getComparator() {
		return comparator;
	}
	
	public void setComparator(Comparator comparator) {
		this.comparator = comparator;
	}
	/**
	 * @return the numReducers
	 */
	public int getNumReducers() {
		return numReducers;
	}
	/**
	 * @param numReducers the numReducers to set
	 */
	public void setNumReducers(int numReducers) {
		this.numReducers = numReducers;
	}
	public int getNumMapper() {
		return numMapper;
	}
	public void setNumMapper(int numMapper) {
		this.numMapper = numMapper;
	}
}
