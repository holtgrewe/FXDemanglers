package de.seqan.knime.demangler.fx;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.port.PortObjectSpec;

import au.com.bytecode.opencsv.CSVReader;

import com.genericworkflownodes.knime.mime.demangler.IDemangler;

public class FXFQStatsDemangler implements IDemangler {

	private static final long serialVersionUID = -581916473275602245L;
	
	private static String COLUMN = "Column";
	private static String COUNT = "Count";
	private static String MIN = "Minimal Quality";
	private static String MAX = "Maximal Quality";
	private static String SUM = "Quality SUm";
	private static String MEAN = "Quality Mean";
	private static String Q1 = "1st Quartile Scoe";
	private static String MEDIAN = "Median Score";
	private static String Q3 = "3rd Quartile Score";
	private static String IQR = "Inter-Quartile Range";
	private static String LEFT_WHISKER = "Left Whisker";
	private static String RIGHT_WHISKER = "Right Whisker";
	private static String A_COUNT = "A Count";
	private static String C_COUNT = "C Count";
	private static String G_COUNT = "G Count";
	private static String T_COUNT = "T Count";
	private static String N_COUNT = "N Count";
	
	private static int COLUMN_COUNT = 17;
	
	@Override
	public String getMIMEType() {
		return "fq_stats_tsv";
	}

	@Override
	public DataTableSpec getTableSpec() {
		DataColumnSpec[] allColSpecs = new DataColumnSpec[COLUMN_COUNT];

		allColSpecs[0] = new DataColumnSpecCreator(COLUMN, IntCell.TYPE).createSpec();
		allColSpecs[1] = new DataColumnSpecCreator(COUNT, IntCell.TYPE).createSpec();
		allColSpecs[2] = new DataColumnSpecCreator(MIN, IntCell.TYPE).createSpec();
		allColSpecs[3] = new DataColumnSpecCreator(MAX, IntCell.TYPE).createSpec();
		allColSpecs[4] = new DataColumnSpecCreator(SUM, IntCell.TYPE).createSpec();
		allColSpecs[5] = new DataColumnSpecCreator(MEAN, DoubleCell.TYPE).createSpec();
		allColSpecs[6] = new DataColumnSpecCreator(Q1, DoubleCell.TYPE).createSpec();
		allColSpecs[7] = new DataColumnSpecCreator(MEDIAN, DoubleCell.TYPE).createSpec();
		allColSpecs[8] = new DataColumnSpecCreator(Q3, DoubleCell.TYPE).createSpec();
		allColSpecs[9] = new DataColumnSpecCreator(IQR, DoubleCell.TYPE).createSpec();
		allColSpecs[10] = new DataColumnSpecCreator(LEFT_WHISKER, DoubleCell.TYPE).createSpec();
		allColSpecs[11] = new DataColumnSpecCreator(RIGHT_WHISKER, DoubleCell.TYPE).createSpec();
		allColSpecs[12] = new DataColumnSpecCreator(A_COUNT, IntCell.TYPE).createSpec();
		allColSpecs[13] = new DataColumnSpecCreator(C_COUNT, IntCell.TYPE).createSpec();
		allColSpecs[14] = new DataColumnSpecCreator(G_COUNT, IntCell.TYPE).createSpec();
		allColSpecs[15] = new DataColumnSpecCreator(T_COUNT, IntCell.TYPE).createSpec();
		allColSpecs[16] = new DataColumnSpecCreator(N_COUNT, IntCell.TYPE).createSpec();

		DataTableSpec outputSpec = new DataTableSpec(allColSpecs);

		return outputSpec;
	}

	@Override
	public PortObjectSpec getPortOjectSpec() {
		return new DataTableSpec();
	}

	@Override
	public Iterator<DataRow> demangle(URI file) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file.toURL().openConnection().getInputStream()));
			CSVReader reader = new CSVReader(bufferedReader, '\t');
			return new DemangleImpl(file, reader);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException("Demangling FX FASTQ TSV from file "
					+ new File(file).getAbsolutePath() + " failed!");
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Demangling FX FASTQ TSV from file "
					+ new File(file).getAbsolutePath() + " failed!");
		}
	}

	@Override
	public void mangle(BufferedDataTable table, URI file) {
		// TODO Auto-generated method stub
	}
	
	public static class DemangleImpl implements Iterator<DataRow> {

		URI file = null;
		CSVReader reader = null;
		String nextLine[];
		int rowIndex = 0;
		
		DemangleImpl(URI file, CSVReader reader) {
			this.file = file;
			this.reader = reader;
			
			try {
				// Skip header.
				nextLine = reader.readNext();
				// Read first line.
				if (nextLine != null)
					nextLine = reader.readNext();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Demangling FX FASTQ TSV from file "
						+ new File(file).getAbsolutePath() + " failed!");
			}
		}
		
		@Override
		public boolean hasNext() {
			return nextLine != null;
		}

		@Override
		public DataRow next() {
			if (nextLine == null)
				return null;
			
			DataCell[] rowCells = new DataCell[COLUMN_COUNT];
			
			rowCells[0] = new IntCell(Integer.parseInt(nextLine[0]));
			rowCells[1] = new IntCell(Integer.parseInt(nextLine[1]));
			rowCells[2] = new IntCell(Integer.parseInt(nextLine[2]));
			rowCells[3] = new IntCell(Integer.parseInt(nextLine[3]));
			rowCells[4] = new IntCell(Integer.parseInt(nextLine[4]));
			rowCells[5] = new DoubleCell(Double.parseDouble(nextLine[5]));
			rowCells[6] = new DoubleCell(Double.parseDouble(nextLine[6]));
			rowCells[7] = new DoubleCell(Double.parseDouble(nextLine[7]));
			rowCells[8] = new DoubleCell(Double.parseDouble(nextLine[8]));
			rowCells[9] = new DoubleCell(Double.parseDouble(nextLine[9]));
			rowCells[10] = new DoubleCell(Double.parseDouble(nextLine[10]));
			rowCells[11] = new DoubleCell(Double.parseDouble(nextLine[11]));
			rowCells[12] = new IntCell(Integer.parseInt(nextLine[12]));
			rowCells[13] = new IntCell(Integer.parseInt(nextLine[13]));
			rowCells[14] = new IntCell(Integer.parseInt(nextLine[14]));
			rowCells[15] = new IntCell(Integer.parseInt(nextLine[15]));
			rowCells[16] = new IntCell(Integer.parseInt(nextLine[16]));

			DataRow result = new DefaultRow(String.format("Row %d", rowIndex++), rowCells);
			
			try {
				// Read next line.
				nextLine = reader.readNext();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Demangling FX FASTQ TSV from file "
						+ new File(file).getAbsolutePath() + " failed!");
			}
			
			return result;
		}

		private void setStringCell(String string, DataCell[] rowCells, int cellIndex) {
			if (string != null) {
				rowCells[cellIndex] = new StringCell(string);
			} else {
				rowCells[cellIndex] = new StringCell("");
			}
		}

		@Override
		public void remove() {
			// nop
		}
	}
}
