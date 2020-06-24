package ra.infovault;

public class ExternalStorageNotAvailable extends Exception {

    public ExternalStorageNotAvailable() {
    }

    public ExternalStorageNotAvailable(String message) {
        super(message);
    }
}
