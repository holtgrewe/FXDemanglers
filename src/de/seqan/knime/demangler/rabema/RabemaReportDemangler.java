package de.seqan.knime.demangler.rabema;

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

public class RabemaReportDemangler implements IDemangler {

	private static final long serialVersionUID = -581916473275602245L;
	
	// error_rate\tnum_max\tnum_found\tpercent_found\tnorm_max\tnorm_found\tpercent_norm_found\n
	private static String ERROR_RATE = "Error Rate";
	private static String NUM_MAX = "Number of Interval";
	private static String NUM_FOUND = "Found Intervals";
	private static String PERCENT_FOUND = "Percent of Found Intervals";
	private static String NORM_MAX = "Normalized Number of Intervals";
	private static String NORM_FOUND = "Normalized Found Intervals";
	private static String PERCENT_NORM_FOUND = "Percent of Normalized Found Intervals";
	
	private static int COLUMN_COUNT = 7;
	
	@Override
	public String getMIMEType() {
		return "rabema_report_tsv";
	}

	@Override
	public DataTableSpec getTableSpec() {
		DataColumnSpec[] allColSpecs = new DataColumnSpec[COLUMN_COUNT];

		allColSpecs[0] = new DataColumnSpecCreator(ERROR_RATE, IntCell.TYPE).createSpec();
		allColSpecs[1] = new DataColumnSpecCreator(NUM_MAX, IntCell.TYPE).createSpec();
		allColSpecs[2] = new DataColumnSpecCreator(NUM_FOUND, IntCell.TYPE).createSpec();
		allColSpecs[3] = new DataColumnSpecCreator(PERCENT_FOUND, DoubleCell.TYPE).createSpec();
		allColSpecs[4] = new DataColumnSpecCreator(NORM_MAX, DoubleCell.TYPE).createSpec();
		allColSpecs[5] = new DataColumnSpecCreator(NORM_FOUND, DoubleCell.TYPE).createSpec();
		allColSpecs[6] = new DataColumnSpecCreator(PERCENT_NORM_FOUND, DoubleCell.TYPE).createSpec();

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
			// Construct buffered reader.
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file.toURL().openConnection().getInputStream()));
			// Skip over comment lines.
			while (true)
			{
				bufferedReader.mark(1);
				int c = bufferedReader.read();  // read single char
				if (c == -1)
				{
					break;  // EOF
				}
				else if (c != '#')
				{
					// not a comment, go back to line start and continue with CSVReader 
					bufferedReader.reset();
					break;
				}
				else
				{
					// is a comment line, skip line
					bufferedReader.readLine();
				}
			}
			bufferedReader.mark(1000);
			String l = bufferedReader.readLine();
			bufferedReader.reset();
			// Create CSVReader from first non-comment position.
			CSVReader reader = new CSVReader(bufferedReader, '\t');
			return new DemangleImpl(file, reader);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException("Demangling Rabema Report TSV from file "
					+ new File(file).getAbsolutePath() + " failed!");
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Demangling Rabema Report TSV from file "
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
				throw new RuntimeException("Demangling Rabema Report TSV from file "
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
			rowCells[3] = new DoubleCell(Double.parseDouble(nextLine[3]));
			rowCells[4] = new DoubleCell(Double.parseDouble(nextLine[4]));
			rowCells[5] = new DoubleCell(Double.parseDouble(nextLine[5]));
			rowCells[6] = new DoubleCell(Double.parseDouble(nextLine[6]));

			DataRow result = new DefaultRow(String.format("Row %d", rowIndex++), rowCells);
			
			try {
				// Read next line.
				nextLine = reader.readNext();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Demangling Rabema Report TSV from file "
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
