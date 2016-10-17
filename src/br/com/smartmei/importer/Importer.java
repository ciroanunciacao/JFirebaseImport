package br.com.smartmei.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import br.com.smartmei.importer.io.ProgressInputStream;
import br.com.smartmei.importer.worker.ChunkSaver;

public class Importer {
	
	private final int CHUNK_SIZE = (1024 * 1024); 
	private final int DEFAULT_THREAD_SIZE = 8;

	private HashMap<String, Object> batch;
	
	private DatabaseReference reference;

	public static void main(String... args) {
		Importer importer = new Importer();
		try {
			importer.init(args);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

	public void init(String... args) throws IOException {
		String inputFilename = "";
		String databaseURL = "";
		String path = "";
		String serviceAccount = "";
		int threads = DEFAULT_THREAD_SIZE;

		if (args.length > 0) {
			for (Iterator<String> i = Arrays.asList(args).iterator(); i.hasNext(); ) {
                String option = i.next();
                if (option.equals("--json")) {
                	inputFilename = i.next();
                } else if (option.equals("--database_url")) {
                	databaseURL = i.next();
                } else if (option.equals("--path")) {
                	path = i.next();
                }  else if (option.equals("--service_account")) {
                	serviceAccount = i.next();
                } else if (option.equals("--threads")) {
                	try {
                		threads = Integer.parseInt(i.next());
					} catch (Exception e) {
						threads = DEFAULT_THREAD_SIZE;
					}
                } else {
                    this.printUsage();
                    return;
                }
            }
			
			if (!inputFilename.isEmpty() && !databaseURL.isEmpty() && !path.isEmpty() && !serviceAccount.isEmpty()) {
				
				FirebaseOptions options = new FirebaseOptions.Builder()
					    .setDatabaseUrl(databaseURL)
					    .setServiceAccount(new FileInputStream(serviceAccount))
					    .build();
				
				FirebaseApp.initializeApp(options);
				
				
				
				this.reference = FirebaseDatabase.getInstance().getReference(path);

				File inputFile = new File(inputFilename);
				InputStream in = new FileInputStream(inputFile);
				ProgressInputStream progressInputStream = new ProgressInputStream(in, inputFile.length());			

				progressInputStream.addListener(
	    				new ProgressInputStream.Listener() {
	    					@Override
	    					public void onProgressChanged(int percentage, long elapsedTime, long eta, long bytesRead) {
	    						updateProgress(percentage, elapsedTime, eta, bytesRead);
	    					}
	    				}
	        		);
				
				JsonReader reader = new JsonReader(new InputStreamReader(progressInputStream, "UTF-8"));
				try {
					this.process(reader, progressInputStream, threads);
				}  finally {
					reader.close();
				}
			} else {
        		this.printUsage();
        	}
		} else {
    		this.printUsage();
    	}
	}
	
	public void process(JsonReader reader, ProgressInputStream progressInputStream, int threads) throws IOException {		
		ArrayList<Map<String, Object>> savingChunks = new ArrayList<Map<String, Object>>();

		long lastBytes = 0;
		long currentByteChunk = 0; 

		while (true) {
            JsonToken token = reader.peek();

            if (progressInputStream.getBytesRead() - lastBytes > 0) {
            	currentByteChunk += (progressInputStream.getBytesRead() - lastBytes);
            }

            lastBytes = progressInputStream.getBytesRead();
                        
            if ((currentByteChunk >= CHUNK_SIZE) && (null != batch) && (!batch.isEmpty())) {
            	currentByteChunk = 0;
            	
            	Map<String, Object> data = new HashMap<String, Object>(this.batch);
            	savingChunks.add(data);

            	batch = null;
    		}
            
            if (savingChunks.size() >= threads) {
            	this.saveChunks(savingChunks);
            	savingChunks.clear();
            	savingChunks.trimToSize();
            }
            
            String firebasePath = convertPath(reader.getPath());
            switch (token) {
	            case BEGIN_OBJECT:
	            	reader.beginObject();
	            	break;
	            case END_OBJECT:
	            	reader.endObject();
	            	break;
	            case BEGIN_ARRAY:	            	
	                reader.beginArray();
	                break;
	            case END_ARRAY:
	                reader.endArray();
	                break;
	            case NAME:
	                reader.nextName();
	                break;
	            case STRING:
	                String s = reader.nextString();
	                this.addValue(firebasePath, s);
	                break;
	            case NUMBER:
	                String n = reader.nextString();	                
	                this.addValue(firebasePath, new BigDecimal(n));
	                break;
	            case BOOLEAN:
	                boolean b = reader.nextBoolean();
	                this.addValue(firebasePath, b);
	                break;
	            case NULL:
	                reader.nextNull();
	                break;
	            case END_DOCUMENT:
	            	if (savingChunks.size() > 0) {
	                	this.saveChunks(savingChunks);
	                	savingChunks.clear();
	                	savingChunks.trimToSize();
	                }
	                return;
            }
		}
	}

	private String convertPath(String path) {
		String firebasePath = path.replaceAll("\\[|\\]\\.|\\]", ".").replaceAll("\\$\\.|\\$$|\\.", "/");
		return firebasePath;
	}
	
	@SuppressWarnings("unchecked")
	private void addValue(String firebasePath, Object value) {
		if (null == this.batch) {
			this.batch = new HashMap<String, Object>();
		}
		Map<String, Object> map = this.batch;

		String[] keys = firebasePath.split("/");
		
		int lastIndex = (keys.length - 1);
		
		for (int i = 0; i < keys.length; i++) {
			String key = keys[i];
			
			if (key.trim().isEmpty()) continue;
			
			if (i < lastIndex) {
				if (!map.containsKey(key)) {
					map.put(key, new HashMap<String, Object>());
				}
				map = (Map<String, Object>)map.get(key);
			} else {
				map.put(key, value);
			}
		}
	}
	
	private void saveChunks(List<Map<String, Object>> chunks) {
		CountDownLatch latch = new CountDownLatch(chunks.size());
		
		ExecutorService executor = Executors.newFixedThreadPool(chunks.size());
		
		for (Map<String, Object> map : chunks) {
			executor.execute(new ChunkSaver(latch, this.reference, map));
		}

		try {
	        latch.await();	        
	        executor.shutdown();
	    } catch (InterruptedException e) {
	    	System.err.println("Data could not be saved " + e.getMessage());
	    } finally {
			executor = null;
		}
	}

	public void updateProgress(double progress, long elapsedTime, long eta, long bytesRead) {
		SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		
	    final int width = 50;
	    final double percentage = (progress / 100);

	    System.out.print("\r[");
	    int i = 0;
	    for (; i <= (int)(percentage * width); i++) {
	      System.out.print("#");
	    }
	    for (; i < width; i++) {
	      System.out.print("-");
	    }

	    System.out.print(
	    		String.format(
		    		"] - %.0f%% - Elapsed: %s - ETA: %s - Read: %s", 
		    		progress, 
		    		formatter.format(new Date(elapsedTime)), 
		    		formatter.format(new Date(eta)),
		    		this.humanReadableByteCount(bytesRead, false)
	    		)
	    	);
	}

	private String humanReadableByteCount(long bytes, boolean si) {
	    int unit = si ? 1000 : 1024;
	    if (bytes < unit) return bytes + " B";
	    int exp = (int) (Math.log(bytes) / Math.log(unit));
	    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
	    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

	private void printUsage() {
		System.err.println("Usage: JFirebaseImport [options]");
		System.err.println(" --database_url <url>");
		System.err.println(" --path <path>");
        System.err.println(" --json <file>");
        System.err.println(" --service_account <file>");
        System.err.println(" --threads <threads> - Number of threads");
        System.out.println("");
	}
}
