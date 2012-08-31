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
import org.knime.core.data.url.MIMEType;
import org.knime.core.data.url.port.MIMEURIPortObjectSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.port.PortObjectSpec;

import au.com.bytecode.opencsv.CSVReader;

import com.genericworkflownodes.knime.mime.demangler.IDemangler;

public class FXSamCoverageDemangler implements IDemangler {

	private static final long serialVersionUID = -581916473275602245L;
	
	private static String BIN = "Global Bin No";
	private static String REF_NAME = "Reference";
	private static String REF_BIN = "Bin No";
	private static String BIN_BEGIN = "Bin Begin";
	private static String BIN_LENGTH = "Bin Length";
	private static String COVERAGE = "Coverage Depth";
	private static String CG_CONTENT = "CG Content";
	
	private static int COLUMN_COUNT = 7;
	
	@Override
	public MIMEType getMIMEType() {
		return new MIMEType("sam.coverage.tsv");
	}

	@Override
	public DataTableSpec getTableSpec() {
		DataColumnSpec[] allColSpecs = new DataColumnSpec[COLUMN_COUNT];

		allColSpecs[0] = new DataColumnSpecCreator(BIN, IntCell.TYPE).createSpec();
		allColSpecs[1] = new DataColumnSpecCreator(REF_NAME, StringCell.TYPE).createSpec();
		allColSpecs[2] = new DataColumnSpecCreator(REF_BIN, IntCell.TYPE).createSpec();
		allColSpecs[3] = new DataColumnSpecCreator(BIN_BEGIN, IntCell.TYPE).createSpec();
		allColSpecs[4] = new DataColumnSpecCreator(BIN_LENGTH, IntCell.TYPE).createSpec();
		allColSpecs[5] = new DataColumnSpecCreator(COVERAGE, IntCell.TYPE).createSpec();
		allColSpecs[6] = new DataColumnSpecCreator(CG_CONTENT, DoubleCell.TYPE).createSpec();

		DataTableSpec outputSpec = new DataTableSpec(allColSpecs);

		return outputSpec;
	}

	@Override
	public PortObjectSpec getPortOjectSpec() {
		return new MIMEURIPortObjectSpec(getMIMEType());
	}

	@Override
	public Iterator<DataRow> demangle(URI file) {
		try {
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file.toURL().openConnection().getInputStream()));
			CSVReader reader = new CSVReader(bufferedReader, '\t');
			return new DemangleImpl(file, reader);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException("Demangling SAM coverage TSV from file "
					+ new File(file).getAbsolutePath() + " failed!");
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Demangling SAM coverage TSV from file "
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
				throw new RuntimeException("Demangling SAM coverage TSV from file "
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
			rowCells[1] = new StringCell(nextLine[1]);
			rowCells[2] = new IntCell(Integer.parseInt(nextLine[2]));
			rowCells[3] = new IntCell(Integer.parseInt(nextLine[3]));
			rowCells[4] = new IntCell(Integer.parseInt(nextLine[4]));
			rowCells[5] = new IntCell(Integer.parseInt(nextLine[5]));
			rowCells[6] = new DoubleCell(Double.parseDouble(nextLine[6]));
			
			DataRow result = new DefaultRow(String.format("Row %d", rowIndex++), rowCells);
			
			try {
				// Read next line.
				nextLine = reader.readNext();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Demangling SAM coverage TSV from file "
						+ new File(file).getAbsolutePath() + " failed!");
			}
			
			return result;
		}

		@Override
		public void remove() {
			// nop
		}
	}
}
