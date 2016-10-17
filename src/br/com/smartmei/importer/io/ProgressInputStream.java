package br.com.smartmei.importer.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ProgressInputStream extends FilterInputStream {
	private final long size;
    private long bytesRead;
    private long startedAt;
    private int percent;
    private long elapsedTime;
    private List<Listener> listeners = new ArrayList<>();

    public ProgressInputStream(InputStream in, long size) {
        super(in);
        try {
            this.size = Math.max(size, available());
            if (size == 0) throw new IOException();
        } catch (IOException e) {
            throw new RuntimeException("This reader can only be used on InputStreams with a known size", e);
        }
        bytesRead = 0;
        percent = 0;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }
    
    public long getBytesRead() {
    	return this.bytesRead;
    }

    @Override
    public int read() throws IOException {
        int b = super.read();        
        updateProgress(1);
        return b;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return updateProgress(super.read(b));
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return updateProgress(super.read(b, off, len));
    }

    @Override
    public long skip(long n) throws IOException {
        return updateProgress(super.skip(n));
    }

    @Override
    public void mark(int readLimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    private <T extends Number> T updateProgress(T numBytesRead) {
        if (numBytesRead.longValue() > 0) {
        	if (bytesRead == 0) {
        		this.startedAt = System.currentTimeMillis();
        	}
            bytesRead += numBytesRead.longValue();
            
            long elapsedTime = System.currentTimeMillis() - this.startedAt;
            
            long eta =  0;
            
            double percentProcessed = ((double)bytesRead / (double)size) *  100d;
            
            if ((percentProcessed > 0) && (elapsedTime > 0)) {
            	eta = (long) (((double)elapsedTime / percentProcessed) * (100d - percentProcessed));
            }            

            if ( ((int)percentProcessed > 0) && ((elapsedTime - this.elapsedTime) > 1000) ) {
                percent = (int) percentProcessed;

                this.elapsedTime = elapsedTime;
                for (Listener listener : listeners) {
                    listener.onProgressChanged(percent, this.elapsedTime, eta, bytesRead);
                }
            } else if ((elapsedTime - this.elapsedTime) > 5000) {
            	this.elapsedTime = elapsedTime; 
            	for (Listener listener : listeners) {
                    listener.onProgressChanged(percent, this.elapsedTime, eta, bytesRead);
                }
            }
        }
        return numBytesRead;
    }

    public interface Listener {
        void onProgressChanged(int percentage, long elapsedTime, long eta, long bytesRead);
    }
}
