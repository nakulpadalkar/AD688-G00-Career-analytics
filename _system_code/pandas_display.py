"""Five-row pandas previews for course notebooks and lab reports."""

from IPython.display import Markdown, display


def table(frame, caption=None):
    """Display the first five rows without an index; leave the input unchanged."""
    display(frame.head().style.hide(axis="index"))
    if caption:
        display(Markdown(caption))
