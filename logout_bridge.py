# pages/logout_bridge.py
import time
import streamlit as st
from auth_utils import clear_auth_data

st.set_page_config(page_title="Logging out…", layout="centered", page_icon = "https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
    initial_sidebar_state="collapsed")
st.markdown("### Logging you out…")

# 1) Server-side: expire cookies and wipe this tab's session state
clear_auth_data()

# Remove any prefixed session_state keys for this tab
active_prefix = None
for k in list(st.session_state.keys()):
    if k.endswith("_session_id"):
        active_prefix = k[:-11]
        break
if active_prefix:
    for k in list(st.session_state.keys()):
        if k.startswith(active_prefix):
            del st.session_state[k]

# 2) Client-side: also kill cookies (covers path quirks on some browsers)
import streamlit.components.v1 as components
components.html(
    """
    <script>
    (function(){
      function kill(name){
        var paths=['/','/opening','/closing','/active','/net','/pages',''];
        for (var i=0;i<paths.length;i++){
          document.cookie = name + '=; Max-Age=0; path=' + paths[i] + ';';
        }
      }
      try { kill('auth_data'); kill('session_id'); } catch(e){}
    })();
    </script>
    """,
    height=0,
)

# 3) Server redirect to the real login page (no query params)
time.sleep(0.15)
st.switch_page("login.py")
st.stop()
